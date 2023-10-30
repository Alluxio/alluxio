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

use crate::{bdev::do_async, error::Result};
use futures_lite::future::block_on;
use std::{ffi::c_void, sync::mpsc::Sender};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Action {
    Read,
    Write,
}

#[derive(Debug)]
pub struct Request {
    pub action: Action,
    pub offset: usize,
    pub length: usize,
    pub buf: *mut u8,
    pub arg: *mut c_void,
}

unsafe impl Send for Request {}

impl Request {
    pub fn new(
        action: Action,
        offset: usize,
        length: usize,
        buf: *mut u8,
        arg: *mut c_void,
    ) -> Self {
        Request {
            action,
            offset,
            length,
            buf,
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
            let request = Request::new(action, offset, length, buf.as_mut_ptr(), arg);
            let _ = self.tx.send(request);
        }))
    }
}
