use crate::{bdev::do_async, dma::DmaBuf, error::Result};
use futures_lite::future::block_on;
use std::{ffi::c_void, sync::mpsc::Sender};

#[derive(Debug)]
pub enum Action {
    Read,
    Write,
}

#[derive(Debug)]
pub struct Request {
    pub action: Action,
    pub offset: usize,
    pub length: usize,
    pub buf: DmaBuf,
    pub arg: *mut c_void,
}

unsafe impl Send for Request {}

impl Request {
    pub fn new(
        action: Action,
        offset: usize,
        length: usize,
        buf: &mut [u8],
        arg: *mut c_void,
    ) -> Self {
        Request {
            action,
            offset,
            length,
            buf: DmaBuf {
                len: length,
                ptr: buf.as_mut_ptr(),
            },
            arg,
        }
    }
}

pub struct SpdkProducer {
    tx: Sender<Request>,
}

impl SpdkProducer {
    pub fn new(tx: Sender<Request>) -> Self {
        SpdkProducer { tx }
    }

    pub fn produce(
        &self,
        action: Action,
        offset: usize,
        length: usize,
        buf: &mut [u8],
    ) -> Result<()> {
        block_on(do_async(|arg| {
            let request = Request::new(action, offset, length, buf, arg);
            let _ = self.tx.send(request);
        }))
    }
}
