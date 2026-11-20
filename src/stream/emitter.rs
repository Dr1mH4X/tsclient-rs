use std::collections::VecDeque;
use std::sync::Mutex;

use tokio::sync::mpsc;

/// 每个事件桶在未接入任何订阅者期间最多保留的事件条数（与参考实现 `MAX_KEPT = 256` 对齐）。
pub(crate) const MAX_KEPT: usize = 256;

/// 类型化事件订阅接收端。
pub struct StreamSubscription<E> {
    rx: mpsc::UnboundedReceiver<E>,
}

impl<E> StreamSubscription<E> {
    /// 异步等待下一条事件；发布端释放后返回 `None`。
    pub async fn recv(&mut self) -> Option<E> {
        self.rx.recv().await
    }

    /// 非阻塞尝试读取下一条事件。
    pub fn try_recv(&mut self) -> Result<E, mpsc::error::TryRecvError> {
        self.rx.try_recv()
    }
}

struct EventHubInner<E> {
    next_seq: u64,
    kept: Vec<VecDeque<(u64, E)>>,
    subscribers: Vec<mpsc::UnboundedSender<E>>,
}

/// 支持首订阅者重放（按事件分类各保留最多 [`MAX_KEPT`] 条）的多播事件分发器。
pub(crate) struct EventHub<E> {
    inner: Mutex<EventHubInner<E>>,
    bucket_of: fn(&E) -> Option<usize>,
}

impl<E: Clone> EventHub<E> {
    pub(crate) fn new(num_buckets: usize, bucket_of: fn(&E) -> Option<usize>) -> Self {
        Self {
            inner: Mutex::new(EventHubInner {
                next_seq: 0,
                kept: (0..num_buckets).map(|_| VecDeque::new()).collect(),
                subscribers: Vec::new(),
            }),
            bucket_of,
        }
    }

    /// 发送一条事件；若因无人订阅且缓存溢出而丢弃最旧条目，返回被溢出的桶编号。
    pub(crate) fn emit(&self, event: E) -> Option<usize> {
        let mut inner = self.inner.lock().unwrap();
        inner.subscribers.retain(|tx| !tx.is_closed());
        if !inner.subscribers.is_empty() {
            for tx in &inner.subscribers {
                let _ = tx.send(event.clone());
            }
            return None;
        }
        let bucket_idx = (self.bucket_of)(&event)?;
        let seq = inner.next_seq;
        inner.next_seq += 1;
        let bucket = &mut inner.kept[bucket_idx];
        bucket.push_back((seq, event));
        if bucket.len() > MAX_KEPT {
            bucket.pop_front();
            return Some(bucket_idx);
        }
        None
    }

    /// 订阅事件流；若这是当前第一个活跃订阅者，将按入队顺序重放已缓存在桶中的历史事件。
    pub(crate) fn subscribe(&self) -> StreamSubscription<E> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut inner = self.inner.lock().unwrap();
        inner.subscribers.retain(|s| !s.is_closed());
        if inner.subscribers.is_empty() {
            let mut replay: Vec<(u64, E)> = Vec::new();
            for bucket in &mut inner.kept {
                replay.extend(bucket.drain(..));
            }
            replay.sort_by_key(|(seq, _)| *seq);
            for (_, ev) in replay {
                let _ = tx.send(ev);
            }
        }
        inner.subscribers.push(tx);
        StreamSubscription { rx }
    }
}
