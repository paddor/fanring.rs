use std::thread;

fn main() {
    let (mut producer, mut consumer) = yring::spsc::<u64>(1024);

    let n = 1_000_000;

    let sender = thread::spawn(move || {
        let mut batch = 0u64;
        for i in 0..n {
            while producer.push(i).is_err() {
                producer.flush();
                thread::yield_now();
            }
            batch += 1;
            if batch == 64 {
                producer.flush();
                batch = 0;
            }
        }
        producer.flush();
    });

    let mut received = 0u64;
    while received < n {
        if consumer.prefetch() > 0 {
            while let Some(v) = consumer.pop() {
                assert_eq!(v, received);
                received += 1;
            }
            consumer.release();
        } else {
            thread::yield_now();
        }
    }

    sender.join().unwrap();
    println!("transferred {n} items correctly");
}
