//! One native voice session per account. The shell arbitrates across accounts.
//! Protocol identifiers and offers stay on the runtime thread.
use super::*;
use crate::call_audio::CallAudio;
use crate::model::CallPhase;
use whatsapp_rust::types::call::{CallAction, IncomingCall};
use whatsapp_rust::voip::{CallEvent, CallHandle};

const RING_TIMEOUT: Duration = Duration::from_secs(60);
const SETUP_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct Session {
    id: String,
    peer: ChatId,
    offer: Option<IncomingCall>,
    started: Instant,
    control: Option<mpsc::UnboundedSender<Control>>,
    task: Option<tokio::task::JoinHandle<()>>,
    abandoned_setup: Arc<std::sync::atomic::AtomicBool>,
}

enum Control {
    End,
    Mute(bool),
    Signal(CallAction),
}

impl Session {
    fn matches(&self, id: &str) -> bool {
        self.id == id
    }
}

impl Worker {
    pub(super) fn start_call(&mut self, id: String, peer: ChatId) {
        let error = if self.call.is_some() {
            Some("Another call is already in progress")
        } else if crate::proxy::for_whatsapp().is_some() {
            Some("Voice calls are unavailable while a proxy is configured")
        } else if !matches!(self.status, LinkStatus::Connected) {
            Some("Connect to WhatsApp before starting a call")
        } else if !peer
            .parse::<Jid>()
            .is_ok_and(|jid| jid.is_pn() || jid.is_lid())
        {
            Some("Voice calls are available in one-to-one chats")
        } else {
            None
        };
        if let Some(reason) = error {
            self.emit(Event::CallEnded {
                id,
                reason: reason.into(),
            });
            return;
        }
        self.call = Some(Session {
            id: id.clone(),
            peer,
            offer: None,
            started: Instant::now(),
            control: None,
            task: None,
            abandoned_setup: Default::default(),
        });
        self.call_update(&id, CallPhase::Outgoing, false);
        self.launch_call();
    }

    pub(super) async fn incoming_call(&mut self, incoming: &IncomingCall) {
        if let CallAction::Offer {
            call_id,
            is_video,
            group_jid,
            ..
        } = &incoming.action
        {
            if self
                .call
                .as_ref()
                .and_then(|s| s.offer.as_ref())
                .is_some_and(|offer| offer.action.call_id() == call_id)
            {
                return;
            }
            let ignored = if incoming.offline {
                Some("offline offer")
            } else if !matches!(self.status, LinkStatus::Connected) {
                Some("account disconnected")
            } else if !self.privacy_ready {
                Some("privacy state not ready")
            } else if self.call.is_some() {
                Some("account already has a call")
            } else if *is_video || group_jid.is_some() || incoming.group.is_some() {
                Some("unsupported video or group call")
            } else if crate::proxy::for_whatsapp().is_some() {
                Some("proxy configured")
            } else {
                None
            };
            // Internal numeric account ids identify the runtime, never the caller.
            let account = self.dirs.id.0.parse::<u64>().unwrap_or_default();
            if let Some(reason) = ignored {
                log::info!("call: account={account} incoming ignored: {reason}");
                return;
            }
            log::info!("call: account={account} incoming offered to interface");
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
            let id = format!(
                "incoming-{}",
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            );
            self.call = Some(Session {
                id: id.clone(),
                peer: self.canonical(&incoming.from),
                offer: Some(incoming.clone()),
                started: Instant::now(),
                control: None,
                task: None,
                abandoned_setup: Default::default(),
            });
            self.call_update(&id, CallPhase::Incoming, false);
            return;
        }
        if !matches!(
            incoming.action,
            CallAction::Accept { .. } | CallAction::Reject { .. } | CallAction::Terminate { .. }
        ) {
            return;
        }
        if let Some(session) = self.call.as_ref() {
            if let Some(control) = &session.control {
                let _ = control.send(Control::Signal(incoming.action.clone()));
            } else if session
                .offer
                .as_ref()
                .is_some_and(|offer| offer.action.call_id() == incoming.action.call_id())
                && matches!(
                    incoming.action,
                    CallAction::Terminate { .. } | CallAction::Reject { reason: None, .. }
                )
            {
                let id = session.id.clone();
                self.call_finished(&id, "Call ended".into());
            }
        }
    }

    pub(super) async fn call_ended_elsewhere(&mut self, wire_id: &str) {
        let Some(session) = self.call.as_ref().filter(|session| {
            session
                .offer
                .as_ref()
                .is_some_and(|offer| offer.action.call_id() == wire_id)
        }) else {
            return;
        };
        log::info!("call: resolved on another linked device");
        if session.task.is_none() {
            // The phone already resolved this offer. Never send a reject now.
            let id = session.id.clone();
            self.call_finished(&id, "Call answered or declined on another device".into());
        } else {
            self.stop_call("Call answered or declined on another device")
                .await;
        }
    }

    pub(super) fn ignore_call(&mut self, id: &str) {
        if self
            .call
            .as_ref()
            .is_some_and(|s| s.matches(id) && s.task.is_none())
        {
            self.call_finished(id, "Call unanswered on this device".into());
        }
    }

    pub(super) fn accept_call(&mut self, id: &str) {
        if !self
            .call
            .as_ref()
            .is_some_and(|s| s.matches(id) && s.offer.is_some() && s.task.is_none())
        {
            return;
        }
        if crate::proxy::for_whatsapp().is_some() {
            self.ignore_call(id);
            return;
        }
        log::info!("call: incoming accepted locally; opening audio");
        self.call_update(id, CallPhase::Connecting, false);
        self.launch_call();
    }

    fn launch_call(&mut self) {
        let Some(client) = self.client.clone() else {
            if let Some(session) = &self.call {
                let id = session.id.clone();
                self.call_finished(&id, "Not connected to WhatsApp".into());
            }
            return;
        };
        let Some(session) = self.call.as_mut() else {
            return;
        };
        let (tx, rx) = mpsc::unbounded_channel();
        session.control = Some(tx);
        let id = session.id.clone();
        let peer = session.peer.clone();
        let offer = session.offer.clone();
        let commands = self.commands.clone();
        let abandoned_setup = session.abandoned_setup.clone();
        session.task = Some(tokio::spawn(async move {
            let reason = run_call(
                client,
                peer,
                offer,
                &id,
                commands.clone(),
                rx,
                abandoned_setup,
            )
            .await;
            log::info!("call: session finished: {reason}");
            let _ = commands.send(Command::CallFinished { id, reason });
        }));
    }

    pub(super) fn mute_call(&self, id: &str, muted: bool) {
        if let Some(session) = &self.call
            && session.matches(id)
            && let Some(control) = &session.control
        {
            let _ = control.send(Control::Mute(muted));
        }
    }

    pub(super) async fn end_call(&mut self, id: &str) {
        if self.call.as_ref().is_some_and(|s| s.matches(id)) {
            self.stop_call("Call ended").await;
        }
    }

    pub(super) async fn stop_call(&mut self, reason: &str) {
        self.stop_call_inner(reason, true).await;
    }

    pub(super) async fn stop_call_for_disconnect(&mut self, reason: &str) {
        self.stop_call_inner(reason, false).await;
    }

    async fn stop_call_inner(&mut self, reason: &str, recover: bool) {
        let Some(mut session) = self.call.take() else {
            return;
        };
        log::info!("call: local shutdown requested: {reason}");
        if let Some(control) = session.control.take() {
            let _ = control.send(Control::End);
        } else if let (Some(client), Some(offer)) = (&self.client, &session.offer) {
            let _ = tokio::time::timeout(Duration::from_secs(2), client.voip().reject(offer)).await;
        }
        if let Some(mut task) = session.task.take()
            && tokio::time::timeout(Duration::from_secs(4), &mut task)
                .await
                .is_err()
        {
            task.abort();
            let _ = task.await;
        }
        if recover
            && session
                .abandoned_setup
                .load(std::sync::atomic::Ordering::Acquire)
        {
            self.recover_call_setup();
        }
        self.emit(Event::CallEnded {
            id: session.id,
            reason: reason.into(),
        });
    }

    pub(super) fn call_update(&self, id: &str, phase: CallPhase, muted: bool) {
        if let Some(session) = &self.call
            && session.matches(id)
        {
            self.emit(Event::Call {
                id: id.into(),
                peer: session.peer.clone(),
                phase,
                muted,
            });
        }
    }

    pub(super) fn call_finished(&mut self, id: &str, reason: String) {
        if self.call.as_ref().is_some_and(|s| s.matches(id)) {
            let abandoned = self.call.take().is_some_and(|session| {
                session
                    .abandoned_setup
                    .load(std::sync::atomic::Ordering::Acquire)
            });
            if abandoned {
                self.recover_call_setup();
            }
            self.emit(Event::CallEnded {
                id: id.into(),
                reason,
            });
        }
    }

    fn recover_call_setup(&mut self) {
        if let Some(client) = self.client.clone()
            && matches!(self.status, LinkStatus::Connected)
        {
            // The pinned outgoing facade has no cancellation guard around its
            // offer send. Reconnecting runs upstream's registry and pending-media
            // cleanup. Publish this before CallEnded releases the shell's slot.
            self.set_status(LinkStatus::Connecting);
            tokio::spawn(async move { client.reconnect_immediately().await });
        }
    }

    pub(super) fn pump_calls(&mut self) {
        if let Some(session) = &self.call
            && session.task.is_none()
            && session.started.elapsed() >= RING_TIMEOUT
        {
            // Leave the other linked devices ringing.
            let _ = self.commands.send(Command::IgnoreCall {
                id: session.id.clone(),
            });
        }
    }
}

/// Dropped after the setup future, before the actor reports completion.
/// Returned errors and handles already have upstream cleanup ownership.
struct SetupGuard {
    abandoned: Arc<std::sync::atomic::AtomicBool>,
    armed: bool,
}

impl Drop for SetupGuard {
    fn drop(&mut self) {
        if self.armed {
            self.abandoned
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }
}

async fn run_call(
    client: Arc<Client>,
    peer: ChatId,
    offer: Option<IncomingCall>,
    id: &str,
    commands: mpsc::UnboundedSender<Command>,
    mut control: mpsc::UnboundedReceiver<Control>,
    abandoned_setup: Arc<std::sync::atomic::AtomicBool>,
) -> String {
    // Device handles are constructed only after an explicit outgoing/accept action.
    let opening = tokio::task::spawn_blocking(CallAudio::open);
    let mut muted = false;
    tokio::pin!(opening);
    let device_timeout = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(device_timeout);
    let audio = loop {
        tokio::select! {
            _ = &mut device_timeout => return "Opening the microphone or speaker timed out".into(),
            result = &mut opening => match result {
                Ok(Ok(audio)) => break audio,
                Ok(Err(reason)) => return reason,
                Err(_) => return "Could not open call audio".into(),
            },
            command = control.recv() => match command {
                Some(Control::End) | None => return "Call ended".into(),
                Some(Control::Mute(value)) => muted = value,
                Some(Control::Signal(action)) if matches!(action, CallAction::Terminate { .. })
                    && offer.as_ref().is_some_and(|offer| offer.action.call_id() == action.call_id()) => return "Call ended".into(),
                _ => {}
            }
        }
    };
    log::info!("call: audio devices opened; starting protocol setup");
    audio.set_muted(true);
    let source = audio.source();
    let sink = audio.sink();
    let mut setup_guard = SetupGuard {
        abandoned: abandoned_setup,
        armed: offer.is_none(),
    };
    let setup = async {
        if let Some(offer) = &offer {
            client
                .voip()
                .accept(offer)
                .audio(source, sink)
                .start()
                .await
        } else {
            let jid: Jid = peer.parse().expect("validated call peer");
            client.voip().call(&jid).audio(source, sink).start().await
        }
    };
    // Keep receiving controls during setup; cancellation drops upstream's registration guard.
    tokio::pin!(setup);
    let timeout = tokio::time::sleep(SETUP_TIMEOUT);
    tokio::pin!(timeout);
    let mut pending = Vec::new();
    let handle = loop {
        tokio::select! {
            result = &mut setup => {
                setup_guard.armed = false;
                match result { Ok(handle) => break handle, Err(_) => return "Could not connect the voice call".into() }
            },
            _ = &mut timeout => return "Call connection timed out".into(),
            command = control.recv() => match command {
                Some(Control::End) | None => return "Call ended".into(),
                Some(Control::Mute(value)) => muted = value,
                Some(Control::Signal(action)) => {
                    if offer.as_ref().is_some_and(|offer| offer.action.call_id() == action.call_id())
                        && matches!(action, CallAction::Terminate { .. }) {
                        return "Call ended".into();
                    }
                    if pending.len() < 32 { pending.push(action); }
                },
            }
        }
    };
    log::info!("call: protocol setup completed; waiting for media");
    let events = handle.events();
    let mut accepted = offer.is_some();
    let mut relay = false;
    let mut phase = if accepted {
        CallPhase::Connecting
    } else {
        CallPhase::Ringing
    };
    let mut reason = None;
    for action in pending {
        if action.call_id() == handle.call_id() {
            apply_signal(&action, &mut accepted, &mut reason);
        }
    }
    let _ = commands.send(Command::CallUpdate {
        id: id.into(),
        phase,
        muted,
    });
    let timeout = tokio::time::sleep(RING_TIMEOUT);
    tokio::pin!(timeout);
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    while reason.is_none() {
        tokio::select! {
            _ = handle.wait_ended() => reason = Some("Call media task ended"),
            _ = &mut timeout, if phase != CallPhase::Active => reason = Some("No answer or call connection timed out"),
            _ = tick.tick() => {
                if let Some(error) = audio.take_error() {
                    log::warn!("call: audio device failure: {error}");
                    reason = Some("The microphone or speaker stopped working");
                }
                if crate::proxy::for_whatsapp().is_some() { reason = Some("Call ended because a proxy was configured"); }
            }
            event = events.recv() => match event {
                Ok(CallEvent::RelayAllocated) => {
                    log::info!("call: relay allocated");
                    relay = true;
                },
                Ok(CallEvent::RelayAllocateFailed(_)) => reason = Some("Call relay allocation failed"),
                Ok(CallEvent::RelayAllocateTimedOut) => reason = Some("Call relay allocation timed out"),
                Ok(CallEvent::RelayReconnectTimedOut) => reason = Some("Call relay reconnection timed out"),
                Ok(CallEvent::MediaSetupFailed(_)) => reason = Some("Call media setup failed"),
                Ok(CallEvent::AudioFormatMismatch { .. }) => reason = Some("Call audio formats do not match"),
                Ok(CallEvent::Closed(_)) => reason = Some("Call media connection closed"),
                Err(_) => reason = Some("Call event channel closed"),
                _ => {}
            },
            command = control.recv() => match command {
                Some(Control::End) | None => reason = Some("Call ended"),
                Some(Control::Mute(value)) => {
                    muted = if phase == CallPhase::Active {
                        set_call_muted(&handle, &audio, value).await
                    } else { value };
                    let _ = commands.send(Command::CallUpdate { id: id.into(), phase, muted });
                }
                Some(Control::Signal(action)) => {
                    if action.call_id() == handle.call_id() { apply_signal(&action, &mut accepted, &mut reason); }
                }
            }
        }
        if accepted && relay && phase != CallPhase::Active && reason.is_none() {
            log::info!("call: peer accepted and relay ready");
            phase = CallPhase::Active;
            muted = set_call_muted(&handle, &audio, muted).await;
            let _ = commands.send(Command::CallUpdate {
                id: id.into(),
                phase,
                muted,
            });
        }
    }
    audio.set_muted(true);
    // Also silence a frame already dequeued by the upstream PCM adapter.
    let _ = tokio::time::timeout(Duration::from_millis(250), handle.set_muted(true)).await;
    close_handle(&handle).await;
    reason.unwrap_or("Call ended").into()
}

/// Keep capture closed until upstream confirms unmute. Cancellation or a
/// signaling failure leaves both the controls and the microphone muted.
async fn set_call_muted(handle: &CallHandle, audio: &CallAudio, muted: bool) -> bool {
    audio.set_muted(true);
    let announced = tokio::time::timeout(Duration::from_secs(1), handle.set_muted(muted)).await;
    let effective = muted || !matches!(announced, Ok(Ok(())));
    audio.set_muted(effective);
    effective
}

async fn close_handle(handle: &CallHandle) {
    let _ = tokio::time::timeout(Duration::from_secs(2), handle.terminate()).await;
    handle.hangup_local().await;
}

fn apply_signal(action: &CallAction, accepted: &mut bool, reason: &mut Option<&'static str>) {
    match action {
        CallAction::Accept { .. } => *accepted = true,
        // A busy companion does not mean the person's other devices declined.
        CallAction::Reject { reason: None, .. } => *reason = Some("Call declined"),
        CallAction::Terminate { .. } => *reason = Some("Call ended"),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn busy_companion_does_not_end_the_call() {
        let mut accepted = false;
        let mut reason = None;
        let action = CallAction::Reject {
            call_id: "test".into(),
            call_creator: "1@s.whatsapp.net".parse().unwrap(),
            reason: Some("busy".into()),
        };
        apply_signal(&action, &mut accepted, &mut reason);
        assert_eq!(reason, None);
        let action = CallAction::Reject {
            call_id: "test".into(),
            call_creator: "1@s.whatsapp.net".parse().unwrap(),
            reason: None,
        };
        apply_signal(&action, &mut accepted, &mut reason);
        assert_eq!(reason, Some("Call declined"));
    }
    fn pending(id: &str) -> Session {
        Session {
            id: id.into(),
            peer: "synthetic".into(),
            offer: None,
            started: Instant::now(),
            control: None,
            task: None,
            abandoned_setup: Default::default(),
        }
    }

    #[tokio::test]
    async fn stale_callbacks_and_commands_cannot_end_or_mute_replacement() {
        let (mut worker, events, _, _) = super::super::receipt_tests::worker();
        let (control, mut controls) = mpsc::unbounded_channel();
        let mut session = pending("new");
        session.control = Some(control);
        worker.call = Some(session);
        worker.call_update("old", CallPhase::Active, false);
        worker.call_finished("old", "Old call ended".into());
        worker.mute_call("old", true);
        worker.end_call("old").await;
        assert!(events.try_recv().is_err());
        assert!(controls.try_recv().is_err());
        assert!(worker.call.as_ref().unwrap().matches("new"));
        worker.mute_call("new", true);
        assert!(matches!(controls.try_recv(), Ok(Control::Mute(true))));
        worker.call_update("new", CallPhase::Active, true);
        assert!(matches!(
            events.try_recv(),
            Ok(Event::Call {
                phase: CallPhase::Active,
                muted: true,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn disconnect_waits_for_task_cleanup_and_rejects_late_callback() {
        let (mut worker, events, _, _) = super::super::receipt_tests::worker();
        let (control, mut controls) = mpsc::unbounded_channel();
        let cleaned = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cleaned_task = cleaned.clone();
        let mut session = pending("old");
        session.control = Some(control);
        session.task = Some(tokio::spawn(async move {
            assert!(matches!(controls.recv().await, Some(Control::End)));
            cleaned_task.store(true, std::sync::atomic::Ordering::SeqCst);
        }));
        worker.call = Some(session);
        worker.stop_call("Disconnected").await;
        assert!(cleaned.load(std::sync::atomic::Ordering::SeqCst));
        assert!(worker.call.is_none());
        assert!(matches!(events.try_recv(), Ok(Event::CallEnded { .. })));
        worker.call = Some(pending("new"));
        worker.call_finished("old", "Finished".into());
        assert!(events.try_recv().is_err());
        assert!(worker.call.as_ref().unwrap().matches("new"));
    }

    #[test]
    fn ignoring_other_account_call_preserves_active_session() {
        let (mut worker, events, _, _) = super::super::receipt_tests::worker();
        worker.call = Some(pending("ringing"));
        worker.ignore_call("other");
        assert!(worker.call.is_some());
        worker.ignore_call("ringing");
        assert!(worker.call.is_none());
        assert!(matches!(events.try_recv(), Ok(Event::CallEnded { .. })));
    }

    #[tokio::test]
    async fn elsewhere_end_only_clears_the_matching_incoming_offer() {
        let (mut worker, events, _, _) = super::super::receipt_tests::worker();
        let mut session = pending("local");
        let peer: Jid = "1@s.whatsapp.net".parse().unwrap();
        session.offer = Some(
            IncomingCall::builder()
                .from(peer.clone())
                .stanza_id("stanza".into())
                .timestamp(std::time::SystemTime::UNIX_EPOCH.into())
                .offline(false)
                .action(CallAction::Offer {
                    call_id: "wire".into(),
                    call_creator: peer,
                    caller_pn: None,
                    caller_country_code: None,
                    device_class: None,
                    joinable: false,
                    is_video: false,
                    audio: Vec::new(),
                    group_jid: None,
                })
                .build(),
        );
        worker.call = Some(session);
        worker.call_ended_elsewhere("old-wire").await;
        assert!(worker.call.is_some());
        assert!(events.try_recv().is_err());
        worker.call_ended_elsewhere("wire").await;
        assert!(worker.call.is_none());
        assert!(
            matches!(events.try_recv(), Ok(Event::CallEnded { id, reason })
            if id == "local" && reason.contains("another device"))
        );
    }

    #[tokio::test]
    async fn abandoned_setup_recovers_before_releasing_slot_and_stale_finish_is_ignored() {
        let directory = tempfile::tempdir().unwrap();
        let store = whatsapp_rust::store::SqliteStore::new(
            &directory.path().join("fixture.db").to_string_lossy(),
        )
        .await
        .unwrap();
        // This bot is never run: reconnect has no transport and cannot dial.
        let bot = Bot::builder().with_backend(store).build().await.unwrap();
        let (mut worker, events, _, _) = super::super::receipt_tests::worker();
        worker.client = Some(bot.client());
        worker.status = LinkStatus::Connected;
        let session = pending("new");
        session
            .abandoned_setup
            .store(true, std::sync::atomic::Ordering::Release);
        worker.call = Some(session);
        worker.call_finished("old", "Old setup cancelled".into());
        assert_eq!(worker.status, LinkStatus::Connected);
        assert!(events.try_recv().is_err());
        worker.call_finished("new", "Setup cancelled".into());
        assert!(matches!(
            events.try_recv(),
            Ok(Event::Link(LinkStatus::Connecting))
        ));
        assert!(matches!(events.try_recv(), Ok(Event::CallEnded { id, .. }) if id == "new"));
    }

    #[tokio::test]
    async fn shutdown_does_not_reconnect_an_abandoned_setup() {
        let (mut worker, events, _, _) = super::super::receipt_tests::worker();
        worker.status = LinkStatus::Connected;
        let (control, mut controls) = mpsc::unbounded_channel();
        let mut session = pending("cancelled");
        let abandoned = session.abandoned_setup.clone();
        session.control = Some(control);
        session.task = Some(tokio::spawn(async move {
            let _guard = SetupGuard {
                abandoned,
                armed: true,
            };
            assert!(matches!(controls.recv().await, Some(Control::End)));
        }));
        worker.call = Some(session);
        worker.stop_call_for_disconnect("Disconnected").await;
        assert!(matches!(events.try_recv(), Ok(Event::CallEnded { .. })));
        assert!(events.try_recv().is_err());
    }

    #[test]
    fn only_a_dropped_incomplete_outgoing_setup_requests_recovery() {
        for armed in [false, true] {
            let abandoned = Arc::new(std::sync::atomic::AtomicBool::new(false));
            drop(SetupGuard {
                abandoned: abandoned.clone(),
                armed,
            });
            assert_eq!(abandoned.load(std::sync::atomic::Ordering::Acquire), armed);
        }
    }

    #[test]
    fn session_id_rejects_stale_controls() {
        let session = Session {
            id: "new".into(),
            peer: "synthetic".into(),
            offer: None,
            started: Instant::now(),
            control: None,
            task: None,
            abandoned_setup: Default::default(),
        };
        assert!(!session.matches("old"));
        assert!(session.matches("new"));
    }
}
