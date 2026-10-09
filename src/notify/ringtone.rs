//! One cancellable ringtone for the process-wide incoming call.
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

pub struct Ringtone {
    audible: bool,
    stop: Option<Sender<()>>,
}

impl Ringtone {
    pub fn new(audible: bool) -> Self {
        Self {
            audible,
            stop: None,
        }
    }

    pub fn set_ringing(&mut self, ringing: bool) {
        if !ringing {
            // Closing the channel interrupts the interval and releases the player.
            self.stop.take();
        } else if self.stop.is_none() {
            let (stop, stopped) = mpsc::channel();
            self.stop = Some(stop);
            if self.audible {
                let result = std::thread::Builder::new()
                    .name("call-ringtone".into())
                    .spawn(move || {
                        let Ok(device) = crate::audio::open_output() else {
                            log::warn!("call: could not open ringtone output");
                            return;
                        };
                        let player = rodio::Player::connect_new(device.mixer());
                        ring_until_stopped(&stopped, Duration::from_secs(3), || {
                            let decoder = rodio::Decoder::new(std::io::Cursor::new(super::ALERT))
                                .map_err(|_| ())?;
                            // Never accumulate a backlog if the output device stalls.
                            if player.empty() {
                                player.append(decoder);
                            }
                            Ok(())
                        });
                        player.stop();
                    });
                if result.is_err() {
                    log::warn!("call: could not start ringtone thread");
                }
            }
        }
    }

    #[cfg(test)]
    pub fn is_ringing(&self) -> bool {
        self.stop.is_some()
    }
}

fn ring_until_stopped(
    stopped: &Receiver<()>,
    interval: Duration,
    mut play: impl FnMut() -> Result<(), ()>,
) {
    // Check after opening the device too: an answer during device setup must
    // never produce a late ring. Disconnection means the app or call was dropped.
    while matches!(stopped.try_recv(), Err(mpsc::TryRecvError::Empty)) {
        if play().is_err() {
            break;
        }
        if !matches!(
            stopped.recv_timeout(interval),
            Err(mpsc::RecvTimeoutError::Timeout)
        ) {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rings_repeatedly_and_stop_interrupts_the_wait() {
        let (stop, stopped) = mpsc::channel();
        let (played, sounds) = mpsc::channel();
        let (done, finished) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            ring_until_stopped(&stopped, Duration::from_millis(10), || {
                played.send(()).unwrap();
                Ok(())
            });
            done.send(()).unwrap();
        });
        for _ in 0..2 {
            sounds.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        drop(stop);
        finished.recv_timeout(Duration::from_secs(1)).unwrap();
        thread.join().unwrap();
    }

    #[test]
    fn cancelled_before_device_opens_never_rings() {
        let (stop, stopped) = mpsc::channel();
        drop(stop);
        ring_until_stopped(&stopped, Duration::from_secs(3), || panic!("late ringtone"));
    }
}
