#[macro_use] extern crate log;
extern crate deque;

use std::iter::Fuse;
use std::thread::Thread;
use std::sync::{Arc, Mutex, Condvar};
use std::sync::mpsc::{channel, Receiver};
use deque::{BufferPool, Worker, Data, Empty, Abort};


#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
pub struct ParallelMap<A, B, I> {
    inner: Fuse<I>,
    eof_pair: Arc<(Mutex<bool>, Condvar)>,
    worker: Worker<A>,
    rx: Receiver<B>,
    sent: usize,
    received: usize,
}

impl<'a, A: Send, B: Send, I: Iterator<Item=A>> Iterator for ParallelMap<A, B, I> {
    type Item = B;

    fn next(&mut self) -> Option<B> {
        let &(ref eof_mutex, ref eof_cvar) = &*self.eof_pair;
        if let Some(v) = self.inner.next() {
            self.worker.push(v);
            self.sent += 1;
        } else {
            {
                let mut eof = eof_mutex.lock().unwrap();
                *eof = true;
            }
            if self.received == self.sent {
                return None;
            }
        }
        eof_cvar.notify_all();
        let res = self.rx.recv().unwrap();
        self.received += 1;
        Some(res)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.inner.size_hint();
        let sent_but_not_recvd = self.sent - self.received;
        if let Some(upper) = upper {
            (lower + sent_but_not_recvd, Some(upper + sent_but_not_recvd))
        } else {
            (sent_but_not_recvd, None)
        }
    }

}

pub trait IteratorParallelMapExt<A: Send> : Iterator<Item=A> + Sized {

    fn parallel_map<B,F>(self, f: F, concurrency: usize) -> ParallelMap<A, B, Self>
        where B: Send, F: Send+Sync, F: Fn(A) -> B {
        self.parallel_map_ctx(move |_, v| f(v), concurrency, ())
    }

    fn parallel_map_ctx<B,F,C>(self, f: F, concurrency: usize, ctx: C) -> ParallelMap<A, B, Self>
        where B: Send, F: Send+Sync, F: Fn(&mut C, A) -> B, C: Clone+Send {

        let mut fused = self.fuse();
        let f = Arc::new(f);
        let pool = BufferPool::new();
        let (worker, stealer) = pool.deque();
        let (tx, rx) = channel();
        let eof_pair = Arc::new((Mutex::new(false), Condvar::new()));

        // prefill
        let mut sent = 0;
        for v in fused.by_ref().take(concurrency * 2) {
            worker.push(v);
            sent += 1;
        }

        for thread_id in range(0, concurrency) {
            let tx = tx.clone();
            let stealer = stealer.clone();
            let eof_pair = eof_pair.clone();
            let f = f.clone();
            let mut ctx = ctx.clone();
            Thread::spawn(move || {
                'outer: loop {
                    let v: A;
                    'inner: loop {
                        match stealer.steal() {
                            Data(d) => {
                                if cfg!(test) { debug!("thread {} got data", thread_id); }
                                v = d;
                                break;
                            }
                            Empty => {
                                let &(ref eof_mutex, ref eof_cvar) = &*eof_pair;
                                let eof = eof_mutex.lock().unwrap();
                                if *eof {
                                    break 'outer;
                                } else {
                                    if cfg!(test) { debug!("thread {} waiting for cvar", thread_id); }
                                    let _ = eof_cvar.wait(eof).unwrap();
                                }
                            }
                            Abort => {}
                        }
                    }
                    let res = (*f)(&mut ctx, v);
                    tx.send(res).unwrap();
                }
            });
        }

        ParallelMap {
            inner: fused,
            eof_pair: eof_pair,
            worker: worker,
            rx: rx,
            sent: sent,
            received: 0
        }
    }
}

impl<A, I> IteratorParallelMapExt<A> for I where I: Iterator<Item=A>, A: Send {}


#[cfg(test)]
mod tests {
    extern crate test;

    use std::collections::BTreeSet;
    use std::iter::repeat;
    //use std::sync::{Mutex, Arc};
    use std::sync::mpsc::channel;
    use std::thread::Thread;
    use std::io::timer::sleep;
    use std::time::duration::Duration;
    use super::IteratorParallelMapExt;

    static ITEMS: isize = 100;
    static WORKERS: usize = 5;

    #[test]
    fn test_threads() {
        let stuff: Vec<int> = range(0, ITEMS).collect();

        let (tx, rx) = channel();

        for val in stuff.iter() {
            let tx = tx.clone();
            let val = val.clone();
            Thread::spawn(move || {
                let res = val * 2;
                tx.send(res).unwrap();
            });
        }

        rx.iter().take(stuff.len()).count();
    }

    #[bench]
    fn bench_threads(b: &mut test::Bencher) {
        b.iter(test_threads);
    }

    //#[test]
    //fn test_mutex_iter() {
    //    let stuff: Vec<int> = range(0, ITEMS).collect();
    //    let len = stuff.len();
    //    let iter = Arc::new(Mutex::new(stuff.into_iter()));

    //    let (tx, rx) = channel();

    //    for _ in range(0, WORKERS) {
    //        let tx = tx.clone();
    //        let iter = iter.clone();
    //        Thread::spawn(move || {
    //            loop {
    //                let mut iter = iter.lock().unwrap();
    //                match (*iter).next() {
    //                    Some(v) => {
    //                        //debug!("worker {} got {}", worker_id, v);
    //                        let res = v * 2;
    //                        tx.send(res);
    //                    }
    //                    None => break
    //                }
    //            }
    //            //debug!("worker {} is done", worker_id);
    //        });
    //    }

    //    rx.iter().take(len).count();
    //}

    //#[bench]
    //fn bench_mutex_iter(b: &mut test::Bencher) {
    //    b.iter(test_mutex_iter);
    //}

    #[test]
    fn test_deque_fast_op() {
        let res: BTreeSet<int> = range(0, 1000).parallel_map(|x| { x }, WORKERS).collect();
        let expected: BTreeSet<int> = range(0, 1000).collect();
        assert_eq!(res, expected);
    }

    #[test]
    fn test_deque_slow_op() {
        let res: BTreeSet<int> = range(0, 1000)
            .parallel_map(|x| { sleep(Duration::milliseconds(1));  x }, WORKERS)
            .collect();
        let expected: BTreeSet<int> = range(0, 1000).collect();
        assert_eq!(res, expected);
    }

    #[bench]
    fn bench_deque_minimal_overhead(b: &mut test::Bencher) {
        b.iter(|| {
            range(0i, ITEMS).parallel_map(|x| x * 2, WORKERS).count();
        });
    }

    #[bench]
    fn bench_deque_much_memory(b: &mut test::Bencher) {
        let buf = repeat(0).take(1024 * 1024).collect::<Vec<u8>>();
        b.iter(|| {
            range(0i, ITEMS)
                .map(|_| buf.clone())
                .parallel_map(|_| 2u, WORKERS)
                .count();
        });
    }
}
