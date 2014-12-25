#![feature(phase)]

#[phase(plugin, link)] extern crate log;
extern crate deque;

use deque::{BufferPool, Worker, Data, Empty, Abort};


#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
pub struct ParallelMap<A, B> {
    worker: Worker<A>,
    rx: Receiver<B>,
    sent: uint,
    received: uint
}


impl<'a, A, B: Send> Iterator<B> for ParallelMap<A, B> {

    fn next(&mut self) -> Option<B> {
        if self.received >= self.sent {
            return None;
        }
        let res = self.rx.recv();
        self.received += 1;
        Some(res)
    }

    fn size_hint(&self) -> (uint, Option<uint>) {
        let items_left = self.sent - self.received;
        (items_left, Some(items_left))
    }

}


impl<A: Send, I> IteratorParallelMapExt<A> for I where I: Iterator<A> {}

pub trait IteratorParallelMapExt<A: Send> : Iterator<A> {
    fn parallel_map<B:Send>(self, f: fn(A) -> B, concurrency: uint) -> ParallelMap<A, B> {
        let mut pool = BufferPool::new();
        let (mut worker, mut stealer) = pool.deque();
        let (tx, rx) = channel();

        let mut sent = 0u;
        let mut iter = self;
        for v in iter {
            worker.push(v);
            sent += 1;
        }

        for _ in range(0, concurrency) {
            let tx = tx.clone();
            let stealer = stealer.clone();
            let f = f.clone();
            spawn(move || {
                'outer: loop {
                    let v: A;
                    'inner: loop {
                        match stealer.steal() {
                            Data(d) => { v = d; break },
                            Empty => break 'outer,
                            Abort => continue
                        }
                    }
                    let res = f(v);
                    tx.send(res);
                }
            });
        }

        ParallelMap {
            worker: worker,
            rx: rx,
            sent: sent,
            received: 0
        }
    }
}


#[cfg(test)]
mod tests {
    extern crate test;

    use std::collections::BTreeSet;
    use std::sync::{Mutex, Arc};
    use std::io::timer::sleep;
    use std::time::duration::Duration;
    use std::thread::Thread;
    use deque::{BufferPool, Data, Empty, Abort};
    use super::IteratorParallelMapExt;

    static ITEMS: int = 100;
    static WORKERS: uint = 5;

    #[test]
    fn test_empty() {}

    #[test]
    fn test_threads() {
        let stuff: Vec<int> = range(0, ITEMS).collect();

        let (tx, rx) = channel();

        for val in stuff.iter() {
            let tx = tx.clone();
            let val = val.clone();
            Thread::spawn(move || {
                let res = val * 2;
                tx.send(res);
            }).detach();
        }

        for res in rx.iter().take(stuff.len()) {
            //println!("gathered {}", res);
        }
        //println!("done");
    }

    #[bench]
    fn bench_threads(b: &mut test::Bencher) {
        b.iter(test_threads);
    }

    #[test]
    fn test_mutex_channel() {
        let stuff: Vec<int> = range(0, ITEMS).collect();
        let len = stuff.len();
        let iter = Arc::new(Mutex::new(stuff.into_iter()));

        let (tx, rx) = channel();

        for worker_id in range(0, WORKERS) {
            let tx = tx.clone();
            let iter = iter.clone();
            Thread::spawn(move || {
                loop {
                    let mut iter = iter.lock();
                    match (*iter).next() {
                        Some(v) => {
                            //debug!("worker {} got {}", worker_id, v);
                            let res = v * 2;
                            tx.send(res);
                        }
                        None => break
                    }
                }
                //debug!("worker {} is done", worker_id);
            }).detach();
        }

        for _ in rx.iter().take(len) {
            //debug!("res: {}", res);
        }
    }

    #[bench]
    fn bench_mutex_channel(b: &mut test::Bencher) {
        b.iter(test_mutex_channel);
    }

    #[test]
    fn test_deque() {
        let stuff: Vec<int> = range(0, 1000).collect();
        let res: BTreeSet<int> = run_deque(stuff.clone()).iter().map(|it| *it).collect();
        let expected: BTreeSet<int> = stuff.iter().map(|it| *it * 2).collect();
        assert_eq!(res, expected);
    }

    fn run_deque(stuff: Vec<int>) -> Vec<int> {
        fn times_two(x: int) -> int {
            x * 2
        }
        stuff.clone().into_iter().parallel_map(times_two, WORKERS).collect()
    }

    #[bench]
    fn bench_deque(b: &mut test::Bencher) {
        let stuff: Vec<int> = range(0, ITEMS).collect();
        b.iter(|| {
            run_deque(stuff.clone());    
        });
    }
}
