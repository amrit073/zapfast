//! Standard Opus ports. Negotiation, RTP and encryption remain in whatsapp-rust.
use super::*;
use bytes::Bytes;
use whatsapp_rust::voip::audio::{WaOpusDecoder, WaOpusEncoder};
use whatsapp_rust::voip::{AudioCodec, AudioFormat, EncodedAudioFrame};

pub struct OpusPorts {
    pub source: Receiver<Bytes>,
    pub sink: Sender<EncodedAudioFrame>,
    errors: Receiver<String>,
    tasks: [tokio::task::JoinHandle<()>; 2],
}

impl OpusPorts {
    pub const FORMAT: AudioFormat = AudioFormat::OPUS_16KHZ_60MS;

    pub fn new(audio: &CallAudio) -> Result<Self, String> {
        let mut encoder = WaOpusEncoder::new().map_err(|_| "Could not open call Opus encoder")?;
        let mut decoder = WaOpusDecoder::new().map_err(|_| "Could not open call Opus decoder")?;
        let (encoded, source) = async_channel::bounded(QUEUED_FRAMES);
        let stale = source.clone();
        let (sink, packets) = async_channel::bounded::<EncodedAudioFrame>(QUEUED_FRAMES);
        let (errors_tx, errors) = async_channel::bounded(1);
        let encode_errors = errors_tx.clone();
        let capture = Arc::clone(&audio.capture);
        capture.lock().unwrap_or_else(|p| p.into_inner()).encoded = Some(source.clone());
        let pcm = audio.source();
        let encode = tokio::spawn(async move {
            loop {
                let generation = capture.lock().unwrap_or_else(|p| p.into_inner()).generation;
                let Ok(frame) = pcm.recv().await else { break };
                // Serialize with mute/reset: an already-dequeued frame from the old
                // generation must never escape after the mute queue was cleared.
                let state = capture.lock().unwrap_or_else(|p| p.into_inner());
                if state.generation != generation {
                    continue;
                }
                match encoder.encode(&frame) {
                    Ok(packet) => {
                        if let Err(async_channel::TrySendError::Full(packet)) =
                            encoded.try_send(packet.into())
                        {
                            let _ = stale.try_recv();
                            let _ = encoded.try_send(packet);
                        }
                    }
                    Err(_) => {
                        let _ = encode_errors.try_send("Call Opus encoding failed".into());
                        break;
                    }
                }
            }
        });
        let playout = audio.sink();
        let decode = tokio::spawn(async move {
            while let Ok(frame) = packets.recv().await {
                if frame.codec != AudioCodec::Opus || frame.format != Self::FORMAT {
                    let _ = errors_tx
                        .try_send("The peer selected an unsupported call audio format".into());
                    break;
                }
                // A malformed network packet is packet loss, not a reason to hang up.
                if let Ok(samples) = decoder.decode(&frame.data) {
                    let _ = playout.try_send(samples.to_vec());
                }
            }
        });
        Ok(Self {
            source,
            sink,
            errors,
            tasks: [encode, decode],
        })
    }

    pub fn take_error(&self) -> Option<String> {
        self.errors.try_recv().ok()
    }
}

impl Drop for OpusPorts {
    fn drop(&mut self) {
        self.source.close();
        self.sink.close();
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn opus_round_trip_mute_and_shutdown_use_bounded_ports() {
        static DEVICE: AtomicBool = AtomicBool::new(false);
        let (tx, source) = async_channel::bounded(QUEUED_FRAMES);
        let capture = Arc::new(Mutex::new(Capture::new(tx, source.clone())));
        let (sink, playout) = async_channel::bounded(QUEUED_FRAMES);
        let (_errors, errors) = async_channel::bounded(1);
        let audio = CallAudio {
            capture: Arc::clone(&capture),
            source,
            sink,
            errors,
            stop: None,
            thread: None,
            _devices: DeviceLease::acquire(&DEVICE).unwrap(),
        };
        audio.set_muted(false);
        let ports = OpusPorts::new(&audio).unwrap();
        {
            let mut state = capture.lock().unwrap();
            for n in 0..FRAME {
                state.push((n as f32 * 0.1).sin() * 0.5);
            }
        }
        let packet = tokio::time::timeout(Duration::from_secs(1), ports.source.recv())
            .await
            .unwrap()
            .unwrap();
        ports
            .sink
            .send(
                EncodedAudioFrame::builder()
                    .format(OpusPorts::FORMAT)
                    .codec(AudioCodec::Opus)
                    .data(packet)
                    .payload_type(111)
                    .sequence_number(1)
                    .timestamp(0)
                    .marker(false)
                    .build(),
            )
            .await
            .unwrap();
        let decoded = tokio::time::timeout(Duration::from_secs(1), playout.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decoded.len(), FRAME);
        assert!(decoded.iter().any(|sample| sample.abs() > 100));
        // Muting also clears already-encoded speech, not just PCM waiting to encode.
        {
            let mut state = capture.lock().unwrap();
            for _ in 0..FRAME {
                state.push(0.5);
            }
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while ports.source.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        audio.set_muted(true);
        assert!(ports.source.is_empty());
        let outgoing = ports.source.clone();
        let incoming = ports.sink.clone();
        drop(ports);
        assert!(outgoing.is_closed());
        assert!(incoming.is_closed());
    }
}
