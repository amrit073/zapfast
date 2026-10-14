//! One explicit phone-number linking request at a time, owned by its worker.
use super::*;
use whatsapp_rust::pair_code::PairCodeOptions;

impl Worker {
    pub(super) fn stop_pair_request(&mut self) {
        self.pair_code = None;
        self.pairing_phone = None;
        if let Some((_, task)) = self.pair_request.take() {
            task.abort();
        }
    }

    pub(super) fn request_pair_code(&mut self, phone: String) {
        if self.pair_request.is_some() {
            return;
        }
        if self.status.is_connected() {
            self.emit(Event::Link(self.status.clone()));
            return;
        }
        if !super::super::valid_pair_phone(&phone) {
            self.emit(Event::Error(
                "Enter the whole phone number with its country code, without a leading zero."
                    .into(),
            ));
            return;
        }
        let Some(client) = self.client.clone() else {
            self.emit(Event::Error("Not connected to WhatsApp yet".into()));
            self.emit(Event::Link(self.status.clone()));
            return;
        };
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let request = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.pairing_phone = Some(phone.clone());
        self.pair_code = None;
        let status = self.unlinked();
        self.set_status(status);
        let events = self.wa_sender.clone();
        let task = tokio::spawn(async move {
            // Only explicit user requests reach here. QR rotation never replaces
            // a code somebody may still be entering on their phone.
            client.cancel_pair_code().await;
            let result = client
                .pair_with_code(PairCodeOptions {
                    phone_number: phone,
                    ..Default::default()
                })
                .await
                .map_err(|error| error.to_string());
            // Same queue as PairingCode/Error: an old event cannot overtake
            // its result and invalidate a replacement that follows it.
            let _ = events.send(RuntimeEvent::PairCode { request, result });
        });
        self.pair_request = Some((request, task));
    }

    pub(super) fn finish_pair_code(&mut self, request: u64, result: Result<String, String>) {
        if !self
            .pair_request
            .as_ref()
            .is_some_and(|(current, _)| *current == request)
        {
            return;
        }
        self.pair_request.take();
        match result {
            Ok(code) => self.pair_code = Some(code),
            Err(error) => {
                self.pair_code = None;
                self.pairing_phone = None;
                self.emit(Event::Error(format!(
                    "Could not link by phone number: {error}"
                )));
            }
        }
        let status = self.unlinked();
        self.set_status(status);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending(worker: &mut Worker, request: u64) {
        worker.status = LinkStatus::Unlinked {
            qr: Some("synthetic-qr".into()),
            pair_code: None,
            pairing_phone: None,
        };
        worker.pairing_phone = Some("15550001111".into());
        worker.pair_request = Some((request, tokio::spawn(async {})));
    }

    #[tokio::test]
    async fn repeated_submissions_and_stale_completions_do_not_replace_a_request() {
        let (mut worker, events, _, _) = super::super::receipt_tests::worker();
        pending(&mut worker, 2);
        worker.request_pair_code("15550002222".into());
        assert_eq!(worker.pairing_phone.as_deref(), Some("15550001111"));
        assert!(events.try_recv().is_err());
        worker.finish_pair_code(1, Err("old error".into()));
        assert!(worker.pair_request.is_some());
        assert!(events.try_recv().is_err());
        worker.finish_pair_code(2, Ok("ABCD1234".into()));
        assert_eq!(worker.pair_code.as_deref(), Some("ABCD1234"));
        worker.finish_pair_code(2, Err("duplicate".into()));
        assert_eq!(worker.pair_code.as_deref(), Some("ABCD1234"));
    }

    #[tokio::test]
    async fn cancelled_requests_cannot_restore_codes_or_errors() {
        let (mut worker, events, _, _) = super::super::receipt_tests::worker();
        pending(&mut worker, 1);
        worker.stop_pair_request();
        worker.finish_pair_code(1, Ok("ABCD1234".into()));
        worker.finish_pair_code(1, Err("old error".into()));
        assert!(worker.pair_code.is_none());
        assert!(worker.pairing_phone.is_none());
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn duplicate_protocol_events_cannot_overwrite_a_new_request() {
        let (mut worker, events, _, _) = super::super::receipt_tests::worker();
        pending(&mut worker, 2);
        worker
            .handle_wa_event(Arc::new(wa_events::Event::PairingCode(
                wa_events::PairingCode::builder()
                    .code("OLD12345".into())
                    .timeout(Duration::from_secs(180))
                    .build(),
            )))
            .await;
        worker
            .handle_wa_event(Arc::new(wa_events::Event::PairingCodeError(
                wa_events::PairingCodeError::builder()
                    .error("old failure".into())
                    .build(),
            )))
            .await;
        assert!(worker.pair_code.is_none());
        assert!(worker.pair_request.is_some());
        assert!(events.try_recv().is_err());
        worker.stop_pair_request();
    }

    #[tokio::test]
    async fn qr_rotation_keeps_the_phone_code_and_request_failures_allow_retry() {
        let (mut worker, events, mut commands, _) = super::super::receipt_tests::worker();
        pending(&mut worker, 1);
        worker.finish_pair_code(1, Ok("ABCD1234".into()));
        worker
            .handle_wa_event(Arc::new(wa_events::Event::PairingQrCode(
                wa_events::PairingQrCode::builder()
                    .code("synthetic-qr".into())
                    .timeout(Duration::from_secs(20))
                    .build(),
            )))
            .await;
        assert_eq!(worker.pair_code.as_deref(), Some("ABCD1234"));
        assert!(commands.try_recv().is_err());
        pending(&mut worker, 2);
        while events.try_recv().is_ok() {}
        worker.finish_pair_code(2, Err("Synthetic request failure".into()));
        assert!(worker.pair_code.is_none());
        assert!(worker.pair_request.is_none());
        assert!(worker.pairing_phone.is_none());
        assert!(
            events
                .try_iter()
                .any(|event| matches!(event, Event::Error(_)))
        );
    }

    #[tokio::test]
    async fn refresh_retires_the_code_without_automatically_requesting_another() {
        let (mut worker, events, mut commands, _) = super::super::receipt_tests::worker();
        pending(&mut worker, 1);
        worker.finish_pair_code(1, Ok("ABCD1234".into()));
        while events.try_recv().is_ok() {}
        worker
            .handle_wa_event(Arc::new(wa_events::Event::PairingCodeRefresh(
                wa_events::PairingCodeRefresh::builder()
                    .force_manual(true)
                    .build(),
            )))
            .await;
        assert!(worker.pair_code.is_none());
        assert!(worker.pairing_phone.is_none());
        assert!(commands.try_recv().is_err());
        assert!(
            events
                .try_iter()
                .any(|event| matches!(event, Event::Info(_)))
        );
    }
}
