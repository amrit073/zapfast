//! Live call audio. Only bounded, in-memory PCM crosses the protocol boundary.
//!
//! Device streams belong to one thread, so stopping a call releases both devices
//! without waiting for microphone samples. The adapters never touch the archive.

mod opus;
pub use opus::OpusPorts;

use std::num::NonZero;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use async_channel::{Receiver, Sender};
use rodio::Source;
use rodio::cpal::{
    self,
    traits::{DeviceTrait, HostTrait, StreamTrait},
};

const RATE: u32 = 16_000;
const FRAME: usize = 960;
// Capture queues at most 180 ms; decoded frames can be up to 120 ms each.
// Prefer recent speech when either queue fills.
const QUEUED_FRAMES: usize = 3;
const MAX_PLAYOUT: usize = 1_920;

static DEVICES_IN_USE: AtomicBool = AtomicBool::new(false);

/// A cancelled blocking open keeps ownership until the native call returns and
/// its result is dropped. A replacement call must not open competing streams.
struct DeviceLease(&'static AtomicBool);

impl DeviceLease {
    fn acquire(flag: &'static AtomicBool) -> Result<Self, String> {
        flag.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| Self(flag))
            .map_err(|_| "Call audio is still stopping; try again shortly".to_owned())
    }
}

impl Drop for DeviceLease {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

pub struct CallAudio {
    capture: Arc<Mutex<Capture>>,
    source: Receiver<Vec<i16>>,
    sink: Sender<Vec<i16>>,
    errors: Receiver<String>,
    stop: Option<mpsc::Sender<()>>,
    thread: Option<JoinHandle<()>>,
    played: Arc<AtomicU64>,
    _devices: DeviceLease,
}

impl CallAudio {
    /// Opens default devices with capture muted. Call from a blocking task.
    pub fn open() -> Result<Self, String> {
        let devices = DeviceLease::acquire(&DEVICES_IN_USE)?;
        let _ringtone_released = crate::notify::Ringtone::output_access();
        let played = Arc::new(AtomicU64::new(0));
        let worker_played = Arc::clone(&played);
        let (tx, source) = async_channel::bounded(QUEUED_FRAMES);
        let (sink, rx) = async_channel::bounded(QUEUED_FRAMES);
        let (errors_tx, errors) = async_channel::bounded(1);
        let capture = Arc::new(Mutex::new(Capture::new(tx, source.clone())));
        let capture_worker = Arc::clone(&capture);
        let (stop, stopped) = mpsc::channel();
        let (ready_tx, ready) = mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("call-audio".into())
            .spawn(move || match open_devices(Arc::clone(&capture_worker), rx, errors_tx.clone(), worker_played) {
                Ok((input, output)) => {
                    let mut input = Some(input);
                    let mut recovery = CaptureRecovery::default();
                    if ready_tx.send(Ok(())).is_ok() {
                        while let Err(mpsc::RecvTimeoutError::Timeout) =
                            stopped.recv_timeout(Duration::from_millis(250))
                        {
                            let last = capture_worker.lock().unwrap_or_else(|p| p.into_inner()).last_input;
                            match recovery.check(last.elapsed()) {
                                CaptureHealth::Healthy => {},
                                CaptureHealth::Reopen => {
                                    log::warn!("call: microphone stalled; reopening default input once");
                                    drop(input.take());
                                    match open_input(Arc::clone(&capture_worker), errors_tx.clone()) {
                                        Ok(reopened) => input = Some(reopened),
                                        Err(error) => {
                                            let _ = errors_tx.try_send(error);
                                            break;
                                        }
                                    }
                                },
                                CaptureHealth::Failed => {
                                    let _ = errors_tx.try_send("The call microphone stopped delivering audio after reopening".into());
                                    break;
                                },
                            }
                        }
                    }
                    drop(input);
                    drop(output);
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                }
            })
            .map_err(|_| "Could not start call audio".to_owned())?;
        let result = Self {
            capture,
            source,
            sink,
            errors,
            stop: Some(stop),
            thread: Some(thread),
            played,
            _devices: devices,
        };
        ready
            .recv()
            .map_err(|_| "Call audio stopped while opening devices".to_owned())??;
        Ok(result)
    }

    pub fn source(&self) -> Receiver<Vec<i16>> {
        self.source.clone()
    }
    pub fn sink(&self) -> Sender<Vec<i16>> {
        self.sink.clone()
    }

    /// Clear queued speech and the partial capture frame at either mute edge.
    pub fn set_muted(&self, muted: bool) {
        let mut capture = self.capture.lock().unwrap_or_else(|p| p.into_inner());
        capture.muted = muted;
        capture.reset();
        while self.source.try_recv().is_ok() {}
    }

    /// The backend ends the owning call when a device reports a failure.
    pub fn take_error(&self) -> Option<String> {
        self.errors.try_recv().ok()
    }
}

impl Drop for CallAudio {
    fn drop(&mut self) {
        self.source.close();
        self.sink.close();
        self.stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn open_devices(
    capture: Arc<Mutex<Capture>>,
    playout: Receiver<Vec<i16>>,
    errors: Sender<String>,
    played: Arc<AtomicU64>,
) -> Result<(cpal::Stream, rodio::MixerDeviceSink), String> {
    let output_errors = errors.clone();
    let mut output = rodio::DeviceSinkBuilder::from_default_device()
        .map_err(|_| "No speakers available for the call".to_owned())?
        .with_error_callback(move |_| {
            let _ = output_errors.try_send("The call audio output stopped working".into());
        })
        .open_stream()
        .map_err(|_| "Could not open the call speakers".to_owned())?;
    output.log_on_drop(false);
    output.mixer().add(Playout {
        frames: playout,
        played,
        current: Vec::new().into_iter(),
    });
    let input = open_input(capture, errors)?;
    Ok((input, output))
}

fn open_input(
    capture: Arc<Mutex<Capture>>,
    errors: Sender<String>,
) -> Result<cpal::Stream, String> {
    let device = cpal::default_host()
        .default_input_device()
        .ok_or_else(|| "No microphone available for the call".to_owned())?;
    let supported = device
        .default_input_config()
        .map_err(|_| "The microphone has no supported format".to_owned())?;
    let config = supported.config();
    {
        let mut state = capture.lock().unwrap_or_else(|p| p.into_inner());
        state.reset();
        state.last_input = Instant::now();
        state.input_rate = config.sample_rate;
        state.channels = config.channels;
    }
    let input = match supported.sample_format() {
        cpal::SampleFormat::I8 => input_stream::<i8>(&device, &config, capture, errors),
        cpal::SampleFormat::I16 => input_stream::<i16>(&device, &config, capture, errors),
        cpal::SampleFormat::I24 => input_stream::<cpal::I24>(&device, &config, capture, errors),
        cpal::SampleFormat::I32 => input_stream::<i32>(&device, &config, capture, errors),
        cpal::SampleFormat::I64 => input_stream::<i64>(&device, &config, capture, errors),
        cpal::SampleFormat::U8 => input_stream::<u8>(&device, &config, capture, errors),
        cpal::SampleFormat::U16 => input_stream::<u16>(&device, &config, capture, errors),
        cpal::SampleFormat::U32 => input_stream::<u32>(&device, &config, capture, errors),
        cpal::SampleFormat::U64 => input_stream::<u64>(&device, &config, capture, errors),
        cpal::SampleFormat::F32 => input_stream::<f32>(&device, &config, capture, errors),
        cpal::SampleFormat::F64 => input_stream::<f64>(&device, &config, capture, errors),
        _ => return Err("The microphone format is not supported for calls".into()),
    }
    .map_err(|_| "Could not open the call microphone; check microphone permissions".to_owned())?;
    input
        .play()
        .map_err(|_| "Could not start the call microphone".to_owned())?;
    Ok(input)
}

fn input_stream<T>(
    device: &cpal::Device,
    config: &cpal::StreamConfig,
    capture: Arc<Mutex<Capture>>,
    errors: Sender<String>,
) -> Result<cpal::Stream, cpal::BuildStreamError>
where
    T: cpal::SizedSample,
    f32: cpal::FromSample<T>,
{
    device.build_input_stream(
        config,
        move |samples: &[T], _: &_| {
            // A UI mute operation holds this lock only long enough to clear a frame.
            let mut state = capture.lock().unwrap_or_else(|p| p.into_inner());
            if !samples.is_empty() {
                state.last_input = Instant::now();
            }
            for &sample in samples {
                state.push(sample.to_sample::<f32>());
            }
        },
        move |_| {
            let _ = errors.try_send("The call microphone stopped working".into());
        },
        None,
    )
}

#[derive(Default)]
struct CaptureRecovery {
    reopened: bool,
}

#[derive(Debug, PartialEq)]
enum CaptureHealth {
    Healthy,
    Reopen,
    Failed,
}

impl CaptureRecovery {
    fn check(&mut self, idle: Duration) -> CaptureHealth {
        if idle < Duration::from_secs(5) {
            CaptureHealth::Healthy
        } else if self.reopened {
            CaptureHealth::Failed
        } else {
            self.reopened = true;
            CaptureHealth::Reopen
        }
    }
}

struct Capture {
    tx: Sender<Vec<i16>>,
    stale: Receiver<Vec<i16>>,
    input_rate: u32,
    channels: u16,
    channel_count: u16,
    channel_sum: f32,
    weight: u32,
    sum: f32,
    frame: Vec<i16>,
    muted: bool,
    last_input: Instant,
    generation: u64,
    captured_frames: u64,
    encoded: Option<Receiver<bytes::Bytes>>,
}

impl Capture {
    fn new(tx: Sender<Vec<i16>>, stale: Receiver<Vec<i16>>) -> Self {
        Self {
            tx,
            stale,
            input_rate: RATE,
            channels: 1,
            channel_count: 0,
            channel_sum: 0.0,
            weight: 0,
            sum: 0.0,
            frame: Vec::with_capacity(FRAME),
            muted: true,
            last_input: Instant::now(),
            generation: 0,
            captured_frames: 0,
            encoded: None,
        }
    }
    fn reset(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        while self.stale.try_recv().is_ok() {}
        if let Some(encoded) = &self.encoded {
            while encoded.try_recv().is_ok() {}
        }
        self.channel_count = 0;
        self.channel_sum = 0.0;
        self.weight = 0;
        self.sum = 0.0;
        self.frame.clear();
    }
    fn push(&mut self, value: f32) {
        self.channel_sum += if value.is_finite() {
            value.clamp(-1.0, 1.0)
        } else {
            0.0
        };
        self.channel_count += 1;
        if self.channel_count != self.channels {
            return;
        }
        let mono = if self.muted {
            0.0
        } else {
            self.channel_sum / f32::from(self.channels)
        };
        self.channel_sum = 0.0;
        self.channel_count = 0;
        // Integrate over input sample intervals. Integer phase survives callback
        // boundaries and avoids drift at rates such as 44.1 kHz.
        let mut remaining = RATE;
        while remaining > 0 {
            let take = remaining.min(self.input_rate - self.weight);
            self.sum += mono * take as f32;
            self.weight += take;
            remaining -= take;
            if self.weight == self.input_rate {
                self.frame
                    .push((self.sum / self.input_rate as f32 * 32767.0).round() as i16);
                self.weight = 0;
                self.sum = 0.0;
                if self.frame.len() == FRAME {
                    self.captured_frames += 1;
                    let frame = std::mem::replace(&mut self.frame, Vec::with_capacity(FRAME));
                    if let Err(async_channel::TrySendError::Full(frame)) = self.tx.try_send(frame) {
                        let _ = self.stale.try_recv();
                        let _ = self.tx.try_send(frame);
                    }
                }
            }
        }
    }
}

struct Playout {
    played: Arc<AtomicU64>,
    frames: Receiver<Vec<i16>>,
    current: std::vec::IntoIter<i16>,
}
impl Iterator for Playout {
    type Item = f32;
    fn next(&mut self) -> Option<f32> {
        if let Some(sample) = self.current.next() {
            return Some(f32::from(sample) / 32768.0);
        }
        match self.frames.try_recv() {
            Ok(mut frame) => {
                if self.frames.len() >= QUEUED_FRAMES - 1 {
                    // The device fell behind. Discard queued old speech rather
                    // than making all following speech wait for that backlog.
                    for _ in 1..QUEUED_FRAMES {
                        if let Ok(newer) = self.frames.try_recv() {
                            frame = newer;
                        }
                    }
                }
                if frame.len() > MAX_PLAYOUT {
                    return Some(0.0);
                }
                self.played.fetch_add(1, Ordering::Relaxed);
                self.current = frame.into_iter();
                Some(self.current.next().map_or(0.0, |s| f32::from(s) / 32768.0))
            }
            Err(async_channel::TryRecvError::Closed) => None,
            _ => Some(0.0),
        }
    }
}
impl Source for Playout {
    fn current_span_len(&self) -> Option<usize> {
        None
    }
    fn channels(&self) -> NonZero<u16> {
        NonZero::<u16>::MIN
    }
    fn sample_rate(&self) -> NonZero<u32> {
        NonZero::new(RATE).unwrap()
    }
    fn total_duration(&self) -> Option<Duration> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn capture(rate: u32, channels: u16) -> (Capture, Receiver<Vec<i16>>) {
        let (tx, rx) = async_channel::bounded(QUEUED_FRAMES);
        let mut capture = Capture::new(tx, rx.clone());
        capture.input_rate = rate;
        capture.channels = channels;
        capture.muted = false;
        (capture, rx)
    }
    #[test]
    fn fractional_resampling_keeps_exact_frame_lengths_and_downmixes() {
        for rate in [8_000, 16_000, 44_100, 48_000, 96_000] {
            let (mut capture, rx) = capture(rate, 2);
            for _ in 0..rate * 3 / 50 {
                capture.push(0.25);
                capture.push(0.75);
            }
            let frame = rx.try_recv().unwrap();
            assert_eq!(frame.len(), FRAME);
            assert!(frame.iter().all(|&s| (s - 16384).abs() <= 1));
            assert!(rx.is_empty());
        }
    }
    #[test]
    fn overflow_discards_oldest_speech_and_never_grows() {
        let (mut capture, rx) = capture(RATE, 1);
        for n in 0..10 {
            for _ in 0..FRAME {
                capture.push(n as f32 / 10.0);
            }
        }
        assert_eq!(rx.len(), QUEUED_FRAMES);
        assert!(rx.try_recv().unwrap()[0] > 20_000);
    }
    #[test]
    fn mute_discards_partial_speech_and_sends_silence() {
        let (mut capture, rx) = capture(RATE, 1);
        for _ in 0..FRAME / 2 {
            capture.push(1.0);
        }
        capture.muted = true;
        capture.reset();
        for _ in 0..FRAME {
            capture.push(1.0);
        }
        assert!(rx.try_recv().unwrap().iter().all(|&s| s == 0));
    }
    #[test]
    fn muting_clears_queued_frames_and_drop_stops_device_owner() {
        let (capture, source) = capture(RATE, 1);
        let capture = Arc::new(Mutex::new(capture));
        let (sink, _playout) = async_channel::bounded(QUEUED_FRAMES);
        let (_errors_tx, errors) = async_channel::bounded(1);
        let (stop, stopped) = mpsc::channel();
        let (done_tx, done) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let _ = stopped.recv();
            done_tx.send(()).unwrap();
        });
        static TEST_DEVICES: AtomicBool = AtomicBool::new(false);
        let audio = CallAudio {
            capture: Arc::clone(&capture),
            source: source.clone(),
            sink,
            errors,
            stop: Some(stop),
            thread: Some(thread),
            played: Default::default(),
            _devices: DeviceLease::acquire(&TEST_DEVICES).unwrap(),
        };
        for _ in 0..FRAME {
            capture.lock().unwrap().push(1.0);
        }
        assert_eq!(source.len(), 1);
        audio.set_muted(true);
        assert!(source.is_empty());
        for _ in 0..FRAME {
            capture.lock().unwrap().push(1.0);
        }
        assert!(source.try_recv().unwrap().iter().all(|&s| s == 0));
        capture.lock().unwrap().last_input = Instant::now() - Duration::from_secs(6);
        // Capture recovery belongs to the device thread, not the call's timer.
        assert!(audio.take_error().is_none());
        drop(audio);
        done.try_recv().unwrap();
        assert!(source.is_closed());
    }
    #[test]
    fn a_stalled_microphone_gets_one_reopen_before_failure() {
        let mut recovery = CaptureRecovery::default();
        assert_eq!(recovery.check(Duration::ZERO), CaptureHealth::Healthy);
        assert_eq!(
            recovery.check(Duration::from_secs(5)),
            CaptureHealth::Reopen
        );
        assert_eq!(recovery.check(Duration::ZERO), CaptureHealth::Healthy);
        assert_eq!(
            recovery.check(Duration::from_secs(5)),
            CaptureHealth::Failed
        );
    }

    #[test]
    fn malformed_playout_frame_is_discarded() {
        let (tx, rx) = async_channel::bounded(QUEUED_FRAMES);
        let mut player = Playout {
            played: Default::default(),
            frames: rx,
            current: Vec::new().into_iter(),
        };
        tx.try_send(vec![32767; MAX_PLAYOUT + 1]).unwrap();
        assert_eq!(player.next(), Some(0.0));
        assert_eq!(player.current.len(), 0);
        assert_eq!(player.played.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn device_lease_stays_exclusive_until_native_owner_finishes() {
        static TEST_DEVICES: AtomicBool = AtomicBool::new(false);
        let lease = DeviceLease::acquire(&TEST_DEVICES).unwrap();
        let (stop, stopped) = mpsc::channel();
        let owner = std::thread::spawn(move || {
            let _lease = lease;
            stopped.recv().unwrap();
        });
        // Dropping the caller's interest must not free a still-opening device.
        assert!(DeviceLease::acquire(&TEST_DEVICES).is_err());
        stop.send(()).unwrap();
        owner.join().unwrap();
        assert!(DeviceLease::acquire(&TEST_DEVICES).is_ok());
    }
    #[test]
    fn full_playout_queue_skips_to_recent_speech() {
        let (tx, rx) = async_channel::bounded(QUEUED_FRAMES);
        let mut player = Playout {
            played: Default::default(),
            frames: rx,
            current: Vec::new().into_iter(),
        };
        tx.try_send(vec![1]).unwrap();
        tx.try_send(vec![2]).unwrap();
        tx.try_send(vec![16384]).unwrap();
        assert_eq!(player.next(), Some(0.5));
        assert!(tx.is_empty());
    }
    #[test]
    fn playback_underflow_is_silent_and_closed_channel_finishes() {
        let (tx, rx) = async_channel::bounded(QUEUED_FRAMES);
        let mut player = Playout {
            played: Default::default(),
            frames: rx,
            current: Vec::new().into_iter(),
        };
        assert_eq!(player.next(), Some(0.0));
        tx.try_send(vec![16384, -16384]).unwrap();
        assert_eq!(player.next(), Some(0.5));
        assert_eq!(player.next(), Some(-0.5));
        assert_eq!(player.played.load(Ordering::Relaxed), 1);
        drop(tx);
        assert_eq!(player.next(), None);
    }
}
