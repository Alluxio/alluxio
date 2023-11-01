/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 * 
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 * 
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

pub mod bdev;
pub mod complete;
pub mod consumer;
pub mod dma;
pub mod error;
pub mod producer;

use crate::{
    bdev::BdevDesc,
    complete::LocalComplete,
    consumer::SpdkConsumer,
    dma::DmaBuf,
    error::Result,
    producer::{Action, Request, SpdkProducer},
};
use rand::Rng;
use spdk_sys::spdk_app_stop;
use std::{
    env::args,
    sync::mpsc::{channel, Receiver},
    thread,
    time::{Instant, Duration}, ops::Sub,
};

pub fn test(block_size: usize, repeat_count: usize) {
    let (tx, rx) = channel();
    thread::spawn(|| {
        SpdkConsumer::new()
            .name("rspdk")
            .config_file(&args().nth(1).unwrap())
            .block_on(async_main(rx))
    });
    let producer = SpdkProducer::new(tx);
    let mut rng = rand::thread_rng();
    for i in 0..repeat_count {
        println!("round {} ...", i);
        let mut buf = Vec::with_capacity(block_size);
        unsafe {
            buf.set_len(block_size);
        }
        buf.fill(rng.gen());
        let _ = producer.produce(Action::Write, i * 512, 512, &mut buf);
        let w0 = buf[0];
        buf.fill(0);
        let _ = producer.produce(Action::Read, i * 512, 512, &mut buf);
        let r0 = buf[0];
        assert_eq!(w0, r0, "failure");
    }
    println!("success");
}

pub fn benchmark(block_size: usize, write_percent: i8) {
    let repeat_count = 10;
    let (tx, rx) = channel();
    thread::spawn(|| {
        SpdkConsumer::new()
            .name("rspdk")
            .config_file(&args().nth(1).unwrap())
            .block_on(async_main(rx))
    });
    let producer = SpdkProducer::new(tx);
    let mut rng = rand::thread_rng();
    let start = Instant::now();
    for i in 0..repeat_count {
        let mut buf = Vec::with_capacity(block_size);
        unsafe {
            buf.set_len(block_size);
        }
        let x: i8 = rng.gen::<i8>() % 100;
        if x < write_percent {
            buf.fill(rng.gen());
            let _ = producer.produce(Action::Write, i * 512, 512, &mut buf);
        } else {
            buf.fill(0);
            let _ = producer.produce(Action::Read, i * 512, 512, &mut buf);
        }
    }
    let duration = start.elapsed();
    let duration = duration.sub(Duration::from_secs(1));
    let block_size_str = match block_size {
        0..=1023 => format!("{}B", block_size),
        1024..=1048575 => format!("{}KB", block_size / 1024),
        1048576..=1073741823 => format!("{}MB", block_size / (1024 * 1024)),
        _ => format!("{}GB", block_size / (1024 * 1024 * 1024)),
    };
    println!(
        "Average elapsed time for block size ({}), write : read ({} : {}) is {}ms, {}us, {}us, {}ns/byte.",
        block_size_str,
        write_percent,
        100 - write_percent,
        duration.as_millis() / repeat_count as u128,
        duration.as_micros() / repeat_count as u128,
        duration.as_nanos() / repeat_count as u128,
        duration.as_nanos() / repeat_count as u128 / block_size as u128,
    );
}

async fn async_main(rx: Receiver<Request>) {
    let bdev_desc = BdevDesc::create_desc("NVMe0n1").unwrap();

    for request in rx {
        let dma_buf = DmaBuf::alloc(request.length, 0x1000);
        match request.action {
            Action::Read => {
                let _ = bdev_desc
                    .read(request.offset, request.length, dma_buf.as_mut_ptr())
                    .await;
                unsafe { std::ptr::copy(dma_buf.as_ptr(), request.buf, request.length) };
            }
            Action::Write => {
                unsafe { std::ptr::copy(request.buf, dma_buf.as_mut_ptr(), request.length) };
                let _ = bdev_desc
                    .write(request.offset, request.length, dma_buf.as_mut_ptr())
                    .await;
            }
        };
        let complete = unsafe { &mut *(request.arg as *mut LocalComplete<Result<u64>>) };
        complete.complete(Ok(0));
    }

    unsafe {
        spdk_app_stop(0);
    }
}
