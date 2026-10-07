//! Process-wide arbitration and account-bound native call controls.

use super::App;
use crate::backend::Command;
use crate::model::{AccountId, ActiveCall, CallPhase, Chat, ChatId, ChatKind};

impl App {
    pub fn calls_use_proxy(&self) -> bool {
        !self.settings.proxy.trim().is_empty() || crate::proxy::for_whatsapp().is_some()
    }

    pub fn can_start_call(&self, chat: &Chat) -> bool {
        chat.kind == ChatKind::Direct
            && !self.our_ids().contains(&chat.id.as_str())
            && self.is_connected()
            && !self.app_lock.is_locked()
            && !self.calls_use_proxy()
            && self.call.is_none()
    }

    pub(super) fn matches_call(&self, account: &AccountId, id: &str) -> bool {
        self.call
            .as_ref()
            .is_some_and(|call| call.account_id == *account && call.id == id)
    }

    fn send_call_command(&self, account: &AccountId, command: Command) {
        if let Some(account) = self.accounts.iter().find(|entry| entry.id == *account) {
            account.backend.send(command);
        }
    }

    fn prepare_call_audio(&mut self) {
        self.recording = None;
        self.player.stop();
        self.video.stop();
        self.voice_wanted = None;
        self.voice_chat = None;
        self.video_wanted = None;
    }

    pub(super) fn start_call(&mut self, peer: ChatId) {
        if !self
            .chat(&peer)
            .is_some_and(|chat| self.can_start_call(chat))
        {
            return;
        }
        let id = format!("{:032x}", rand::random::<u128>());
        self.call = Some(ActiveCall {
            account_id: self.account().id.clone(),
            id: id.clone(),
            name: if self.chat(&peer).is_some_and(|chat| chat.locked) {
                "WhatsApp caller".into()
            } else {
                self.display_name(&peer)
            },
            peer: peer.clone(),
            phase: CallPhase::Outgoing,
            muted: false,
        });
        self.prepare_call_audio();
        self.backend.send(Command::StartCall { id, chat: peer });
    }

    pub(super) fn accept_call(&mut self, account: &AccountId, id: &str) {
        if self.app_lock.is_locked()
            || self.calls_use_proxy()
            || !self.matches_call(account, id)
            || !self
                .call
                .as_ref()
                .is_some_and(|call| call.phase == CallPhase::Incoming)
            || !self
                .accounts
                .iter()
                .any(|entry| entry.id == *account && entry.link.is_connected())
        {
            return;
        }
        self.clear_call_notification(account, id);
        self.prepare_call_audio();
        self.call.as_mut().unwrap().phase = CallPhase::Connecting;
        self.send_call_command(account, Command::AcceptCall { id: id.into() });
    }

    /// Keep the slot reserved until the worker has released its devices.
    pub(super) fn stop_call(&mut self) {
        let Some(call) = self.call.as_mut() else {
            return;
        };
        if call.phase == CallPhase::Ending {
            return;
        }
        call.phase = CallPhase::Ending;
        let (account, id) = (call.account_id.clone(), call.id.clone());
        self.clear_call_notification(&account, &id);
        self.send_call_command(&account, Command::EndCall { id });
    }

    pub(super) fn mute_call(&mut self, account: &AccountId, id: &str, muted: bool) {
        if !self.matches_call(account, id)
            || !self
                .call
                .as_ref()
                .is_some_and(|call| call.phase == CallPhase::Active)
        {
            return;
        }
        self.send_call_command(
            account,
            Command::MuteCall {
                id: id.into(),
                muted,
            },
        );
    }

    pub(super) fn handle_call(&mut self, id: String, peer: ChatId, phase: CallPhase, muted: bool) {
        let account = self.account().id.clone();
        if self.matches_call(&account, &id) {
            let call = self.call.as_mut().unwrap();
            // A callback already queued before hangup cannot revive a call.
            if call.phase != CallPhase::Ending {
                // Accepting locally outruns the queued ringing callback too.
                if !(call.phase == CallPhase::Connecting && phase == CallPhase::Incoming) {
                    call.phase = phase;
                }
                call.muted = muted;
            }
            return;
        }
        if phase != CallPhase::Incoming || self.call.is_some() || self.calls_use_proxy() {
            log::info!(
                "call: interface ignored event: incoming={}, occupied={}, proxy={}, hidden={}",
                phase == CallPhase::Incoming,
                self.call.is_some(),
                self.calls_use_proxy(),
                self.events_hidden
            );
            self.backend.send(Command::IgnoreCall { id });
            return;
        }
        log::info!(
            "call: incoming bar shown; account_slot={}, hidden={}",
            self.active + 1,
            self.events_hidden
        );
        let private = self.chat(&peer).is_some_and(|chat| chat.locked);
        let name = if private {
            "WhatsApp caller".into()
        } else {
            self.display_name(&peer)
        };
        self.call = Some(ActiveCall {
            account_id: account.clone(),
            id: id.clone(),
            peer,
            name: name.clone(),
            phase,
            muted,
        });
        self.wants_attention = true;
        if self.account().settings.notifications && !private {
            let waker = self.waker.clone();
            self.notifications.show(
                if self.app_lock.is_locked() {
                    "ZapFast".into()
                } else {
                    name
                },
                "Incoming voice call".into(),
                None,
                crate::settings::NotificationSound::Alert,
                crate::notify::NotificationTarget {
                    account,
                    chat: format!("call:{id}"),
                    message: id,
                },
                self.call_opens.clone(),
                move || waker.wake(),
            );
        }
    }

    fn clear_call_notification(&mut self, account: &AccountId, id: &str) {
        self.notifications.clear(account, &format!("call:{id}"));
    }

    pub(super) fn redact_call_peer(&mut self, peer: &str) {
        let account = self.account().id.clone();
        if let Some(call) = self.call.as_mut()
            && call.account_id == account
            && call.peer == peer
        {
            call.name = "WhatsApp caller".into();
            let id = call.id.clone();
            self.clear_call_notification(&account, &id);
        }
    }

    pub(super) fn handle_call_ended(&mut self, id: &str, reason: &str) {
        let account = self.account().id.clone();
        self.clear_call_notification(&account, id);
        if !self.matches_call(&account, id) {
            return;
        }
        self.call = None;
        if !reason.is_empty() && !self.app_lock.is_locked() {
            self.toast(reason);
        }
    }

    pub(super) fn handle_call_opens(&mut self) {
        let opened =
            std::mem::take(&mut *self.call_opens.lock().unwrap_or_else(|p| p.into_inner()));
        for target in opened {
            if !self.matches_call(&target.account, &target.message) {
                continue;
            }
            // Showing a call notification never answers it or reads a chat.
            self.actions.push(crate::model::Action::ShowWindow);
            if !self.app_lock.is_locked() {
                self.switch_account(&target.account);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{Backend, Event, LinkStatus};
    use crate::model::Action;

    fn app() -> (
        tempfile::TempDir,
        App,
        tokio::sync::mpsc::UnboundedReceiver<Command>,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let (mut app, _) = App::headless(
            crate::paths::AppDirs::under(directory.path()),
            Default::default(),
        );
        let (backend, commands) = Backend::recording();
        app.backend = backend;
        app.link = LinkStatus::Connected;
        app.chats.push(Chat::new(
            "15550000001@s.whatsapp.net".into(),
            "Test caller".into(),
        ));
        (directory, app, commands)
    }

    fn incoming(app: &mut App, id: &str) {
        app.apply_backend_event(
            Event::Call {
                id: id.into(),
                peer: "15550000001@s.whatsapp.net".into(),
                phase: CallPhase::Incoming,
                muted: false,
            },
            true,
        );
    }

    #[test]
    fn accepting_and_ending_wait_for_worker_and_reject_stale_callbacks() {
        let (_directory, mut app, mut commands) = app();
        incoming(&mut app, "first");
        assert!(commands.try_recv().is_err(), "ringing must not open audio");
        let account = app.account().id.clone();
        app.accept_call(&account, "first");
        assert!(matches!(commands.try_recv(), Ok(Command::AcceptCall { id }) if id == "first"));
        incoming(&mut app, "first");
        assert_eq!(app.call.as_ref().unwrap().phase, CallPhase::Connecting);
        app.stop_call();
        assert!(matches!(commands.try_recv(), Ok(Command::EndCall { id }) if id == "first"));
        assert_eq!(app.call.as_ref().unwrap().phase, CallPhase::Ending);
        app.handle_call("first".into(), "ignored".into(), CallPhase::Active, false);
        assert_eq!(app.call.as_ref().unwrap().phase, CallPhase::Ending);
        assert!(!app.can_start_call(&app.chats[0]));
        app.handle_call_ended("first", "Call ended");
        incoming(&mut app, "second");
        app.handle_call_ended("first", "stale");
        app.apply(
            Action::HangUpCall {
                account_id: account,
                call_id: "first".into(),
            },
            &Default::default(),
        );
        assert_eq!(app.call.as_ref().unwrap().id, "second");
        assert!(commands.try_recv().is_err());
    }

    #[test]
    fn calls_are_account_bound_and_competing_incoming_is_ignored() {
        let (_directory, mut app, mut first_commands) = app();
        incoming(&mut app, "first");
        let first = app.account().id.clone();
        let (mut second, _) =
            crate::account::Account::detached(&app.dirs, AccountId("2".into()), Default::default())
                .unwrap();
        let (backend, mut second_commands) = Backend::recording();
        second.backend = backend;
        second.link = LinkStatus::Connected;
        app.accounts.push(second);
        app.active = 1;
        incoming(&mut app, "second");
        assert_eq!(app.call.as_ref().unwrap().account_id, first);
        assert!(
            matches!(second_commands.try_recv(), Ok(Command::IgnoreCall { id }) if id == "second")
        );
        app.handle_call_ended("first", "wrong account");
        assert!(app.call.is_some());
        app.accept_call(&first, "first");
        assert!(
            matches!(first_commands.try_recv(), Ok(Command::AcceptCall { id }) if id == "first")
        );
        assert!(second_commands.try_recv().is_err());
        assert_eq!(app.active, 1);
    }

    #[test]
    fn polling_hidden_account_shows_call_and_accepts_on_its_backend() {
        let (_directory, mut app, mut primary_commands) = app();
        let primary = app.account().id.clone();
        let (mut secondary, _) =
            crate::account::Account::detached(&app.dirs, AccountId("2".into()), Default::default())
                .unwrap();
        let secondary_id = secondary.id.clone();
        let (backend, mut secondary_commands, events) = Backend::recording_with_events();
        secondary.backend = backend;
        secondary.link = LinkStatus::Connected;
        app.accounts.push(secondary);
        events
            .send(Event::Call {
                id: "hidden-call".into(),
                peer: "15550000001@s.whatsapp.net".into(),
                phase: CallPhase::Incoming,
                muted: false,
            })
            .unwrap();
        app.handle_events();
        assert_eq!(app.account().id, primary);
        assert!(!app.events_hidden);
        assert_eq!(app.call.as_ref().unwrap().account_id, secondary_id);
        app.accept_call(&secondary_id, "hidden-call");
        assert!(
            matches!(secondary_commands.try_recv(), Ok(Command::AcceptCall { id }) if id == "hidden-call")
        );
        assert!(primary_commands.try_recv().is_err());
    }

    #[test]
    fn locking_ends_call_and_locked_incoming_never_opens_microphone() {
        let (_directory, mut app, mut commands) = app();
        app.settings.app_lock_hash = Some("fixture verifier".into());
        incoming(&mut app, "first");
        app.lock_app();
        assert!(matches!(commands.try_recv(), Ok(Command::EndCall { .. })));
        app.handle_call_ended("first", "Call ended");
        incoming(&mut app, "second");
        let account = app.account().id.clone();
        app.apply(
            Action::AcceptCall {
                account_id: account.clone(),
                call_id: "second".into(),
            },
            &Default::default(),
        );
        assert!(commands.try_recv().is_err());
        let shown = app.notifications.shown.last().unwrap();
        assert_eq!(shown.title, "ZapFast");
        assert_eq!(shown.body, "Incoming voice call");
        app.call_opens
            .lock()
            .unwrap()
            .push(crate::notify::NotificationTarget {
                account,
                chat: "call:second".into(),
                message: "second".into(),
            });
        app.handle_call_opens();
        assert!(app.actions.contains(&Action::ShowWindow));
        assert!(app.app_lock.is_locked());
        assert!(app.open_chat.is_none());
    }

    #[test]
    fn locking_the_callers_chat_redacts_only_the_originating_account() {
        let (_directory, mut app, _commands) = app();
        incoming(&mut app, "private");
        assert_eq!(app.call.as_ref().unwrap().name, "Test caller");
        let second =
            crate::account::Account::detached(&app.dirs, AccountId("2".into()), Default::default())
                .unwrap()
                .0;
        app.accounts.push(second);
        let mut locked = app.chats[0].clone();
        locked.locked = true;
        app.active = 1;
        app.apply_backend_event(Event::ChatUpdated(Box::new(locked.clone())), false);
        assert_eq!(app.call.as_ref().unwrap().name, "Test caller");
        app.active = 0;
        app.apply_backend_event(Event::ChatUpdated(Box::new(locked)), false);
        assert_eq!(app.call.as_ref().unwrap().name, "WhatsApp caller");
    }

    #[test]
    fn locked_chat_identity_stays_private_and_message_audio_stays_stopped() {
        let (_directory, mut app, mut commands) = app();
        app.chats[0].locked = true;
        incoming(&mut app, "private");
        assert_eq!(app.call.as_ref().unwrap().name, "WhatsApp caller");
        assert!(app.notifications.shown.is_empty());
        let ctx = egui::Context::default();
        app.apply(Action::StartRecording, &ctx);
        app.apply(Action::PlayVideoWhenDownloaded("video".into()), &ctx);
        assert!(app.recording.is_none());
        assert!(app.video_wanted.is_none());
        assert!(commands.try_recv().is_err());
    }

    #[test]
    fn calls_require_direct_other_chat_connection_and_no_proxy() {
        let (_directory, mut app, mut commands) = app();
        assert!(app.can_start_call(&app.chats[0]));
        app.settings.proxy = "socks5://localhost:1080".into();
        app.start_call(app.chats[0].id.clone());
        assert!(commands.try_recv().is_err());
        incoming(&mut app, "blocked");
        assert!(app.call.is_none());
        app.settings.proxy.clear();
        app.me = Some(app.chats[0].id.clone());
        assert!(!app.can_start_call(&app.chats[0]));
        app.me = None;
        app.link = LinkStatus::Connecting;
        assert!(!app.can_start_call(&app.chats[0]));
        app.link = LinkStatus::Connected;
        assert!(!app.can_start_call(&Chat::new("group@g.us".into(), "Group".into())));
    }
}
