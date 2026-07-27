use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

const QUEUE_CAPACITY: usize = 4096;

pub(crate) struct RtpRecord {
    pub(crate) ssrc: u32,
    pub(crate) sequence: u16,
    pub(crate) timestamp: u32,
    pub(crate) packet_bytes: usize,
}

#[derive(Clone)]
pub(crate) struct CaptureSender {
    sender: mpsc::Sender<RtpRecord>,
    metrics: Arc<ProducerMetrics>,
}

pub(crate) struct CaptureDrain {
    stop: oneshot::Sender<()>,
    task: JoinHandle<ConsumerSummary>,
    metrics: Arc<ProducerMetrics>,
}

#[derive(Default)]
struct ProducerMetrics {
    accepted: AtomicU64,
    full_drops: AtomicU64,
    closed_drops: AtomicU64,
    high_water: AtomicUsize,
}

#[derive(Default)]
struct ConsumerSummary {
    records: u64,
    packet_bytes: u64,
    stream_tails: HashMap<u32, (u16, u32)>,
}

pub(crate) fn start() -> (CaptureSender, CaptureDrain) {
    let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
    let (stop, stop_receiver) = oneshot::channel();
    let metrics = Arc::new(ProducerMetrics::default());
    let task = tokio::spawn(consume(receiver, stop_receiver));

    (
        CaptureSender {
            sender,
            metrics: Arc::clone(&metrics),
        },
        CaptureDrain {
            stop,
            task,
            metrics,
        },
    )
}

impl CaptureSender {
    pub(crate) fn try_send(&self, record: RtpRecord) {
        let depth_after_send = self.sender.max_capacity() - self.sender.capacity() + 1;

        match self.sender.try_send(record) {
            Ok(()) => {
                self.metrics.accepted.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .high_water
                    .fetch_max(depth_after_send, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.full_drops.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .high_water
                    .fetch_max(self.sender.max_capacity(), Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.closed_drops.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl CaptureDrain {
    pub(crate) async fn stop(self) {
        let Self {
            stop,
            task,
            metrics,
        } = self;
        let _ = stop.send(());

        match task.await {
            Ok(summary) => Self::report(&metrics, &summary),
            Err(error) => eprintln!("capture consumer task failed: {error}"),
        }
    }

    fn report(metrics: &ProducerMetrics, summary: &ConsumerSummary) {
        println!(
            "Capture queue: {} accepted, {} consumed, {} full drops, {} closed drops, \
             high-water {}/{}, {} packet bytes consumed.",
            metrics.accepted.load(Ordering::Relaxed),
            summary.records,
            metrics.full_drops.load(Ordering::Relaxed),
            metrics.closed_drops.load(Ordering::Relaxed),
            metrics.high_water.load(Ordering::Relaxed),
            QUEUE_CAPACITY,
            summary.packet_bytes,
        );

        let mut streams = summary.stream_tails.iter().collect::<Vec<_>>();
        streams.sort_unstable_by_key(|(ssrc, _)| **ssrc);

        for (ssrc, (sequence, timestamp)) in streams {
            println!(
                "Capture consumer SSRC {ssrc}: last sequence {sequence}, \
                 last RTP timestamp {timestamp}."
            );
        }
    }
}

async fn consume(
    mut receiver: mpsc::Receiver<RtpRecord>,
    mut stop: oneshot::Receiver<()>,
) -> ConsumerSummary {
    let mut summary = ConsumerSummary::default();

    loop {
        tokio::select! {
            biased;

            _ = &mut stop => {
                receiver.close();

                while let Some(record) = receiver.recv().await {
                    summary.observe(record);
                }

                return summary;
            }
            record = receiver.recv() => {
                match record {
                    Some(record) => summary.observe(record),
                    None => return summary,
                }
            }
        }
    }
}

impl ConsumerSummary {
    fn observe(&mut self, record: RtpRecord) {
        self.records += 1;
        self.packet_bytes += record.packet_bytes as u64;
        self.stream_tails
            .insert(record.ssrc, (record.sequence, record.timestamp));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(sequence: u16) -> RtpRecord {
        RtpRecord {
            ssrc: 123,
            sequence,
            timestamp: u32::from(sequence) * 960,
            packet_bytes: 100,
        }
    }

    #[test]
    fn full_queue_is_counted_without_blocking() {
        let (sender, _receiver) = mpsc::channel(1);
        let metrics = Arc::new(ProducerMetrics::default());
        let sender = CaptureSender {
            sender,
            metrics: Arc::clone(&metrics),
        };

        sender.try_send(record(1));
        sender.try_send(record(2));

        assert_eq!(metrics.accepted.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.full_drops.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.high_water.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn closed_queue_is_counted_without_panicking() {
        let (sender, receiver) = mpsc::channel(1);
        let metrics = Arc::new(ProducerMetrics::default());
        let sender = CaptureSender {
            sender,
            metrics: Arc::clone(&metrics),
        };
        drop(receiver);

        sender.try_send(record(1));

        assert_eq!(metrics.accepted.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.closed_drops.load(Ordering::Relaxed), 1);
    }
}
