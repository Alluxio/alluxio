use rand::Rng;
use rspdk::{
    bdev::BdevDesc,
    complete::LocalComplete,
    consumer::{app_stop, SpdkConsumer},
    error::{Result, SpdkError},
    producer::{Action, Request, SpdkProducer},
};
use std::{
    env::args,
    sync::mpsc::{channel, Receiver},
    thread, time::{Duration, SystemTime},
};
use sha1::{Sha1, Digest};

fn main() {
    let (tx, rx) = channel();
    let handle = thread::spawn(|| {
        SpdkConsumer::new()
            .name("test")
            .config_file(&args().nth(1).unwrap())
            .block_on(async_main(rx))
    });

    let producer = SpdkProducer::new(tx);

    let mut rng = rand::thread_rng();
    const M: usize = 64 * 1024 * 1024;
    const N: usize = 2 * 1024;

    let sy_time = SystemTime::now();

    for i in 0..M/N {
        if i % 1024 == 0 {
            println!("current: {}", i);
        }
        // println!("current: {}", i);
        let mut out_buf = Box::new([0; N]);
        let mut in_buf = Box::new([0; N]);

        out_buf.fill_with(|| rng.gen());
        // println!("write: {:?}", {
        //     let mut sha = sha1::Sha1::new();
        //     sha.update(*out_buf);
        //     hex::encode(sha.finalize())
        // });
        let _ = producer.produce(Action::Write, (i % 4096) * N, N, Box::leak(out_buf));
        let _ = producer.produce(Action::Read, (i % 4096) * N, N, &mut *in_buf);
        // println!("read: {:?}", {
        //     let mut sha = sha1::Sha1::new();
        //     sha.update(*in_buf);
        //     hex::encode(sha.finalize())
        // });
        // std::thread::sleep(Duration::from_millis(200));
    }
    println!("{:?}", SystemTime::now().duration_since(sy_time).unwrap().as_secs());
    println!("{:?}", sy_time.elapsed().unwrap().as_secs());

    drop(producer);
    //handle.join().unwrap();
}

async fn async_main(rx: Receiver<Request>) {
    //futures_lite::future::yield_now().await;
    let bdev_desc = BdevDesc::create_desc("Malloc0").unwrap();
    // let bdev_desc = BdevDesc::create_desc("NVMe0n1").unwrap();

    for mut request in rx {
        let _ = match request.action {
            Action::Read => {
                let res : Result<_> = bdev_desc
                    .read(request.offset, request.length, request.buf.as_mut())
                    .await;
                res
            }
            Action::Write => {
                let res : Result<_> = bdev_desc
                    .write(request.offset, request.length, request.buf.as_ref())
                    .await;
                res
            }
        };
        let complete = unsafe { &mut *(request.arg as *mut LocalComplete<Result<u64>>) };
        complete.complete(Ok(0));
    }

    dbg!(bdev_desc.close());
    
    dbg!(app_stop());
}
