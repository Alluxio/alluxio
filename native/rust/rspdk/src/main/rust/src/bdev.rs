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


use crate::{
    complete::LocalComplete,
    error::{Result, SpdkError},
};
use log::*;
use spdk_sys::*;
use std::{
    ffi::{c_void, CString},
    mem::MaybeUninit,
};

#[derive(Debug)]
pub struct IoChannel {
    pub ptr: *mut spdk_io_channel,
}

#[derive(Debug)]
pub struct BdevDesc {
    ptr: *mut spdk_bdev_desc,
}

impl BdevDesc {
    pub fn create_desc(name: &str) -> Result<Self> {
        let cname = CString::new(name).unwrap();
        let mut ptr = MaybeUninit::uninit();
        extern "C" fn callback(
            ty: spdk_bdev_event_type,
            bdev: *mut spdk_bdev,
            event_ctx: *mut c_void,
        ) {
            warn!(
                "bdev callback: type = {:?}, bdev={:?}, ctx={:?}",
                ty, bdev, event_ctx
            );
        }
        let err = unsafe {
            spdk_bdev_open_ext(
                cname.as_ptr(),
                true,
                Some(callback),
                std::ptr::null_mut(),
                ptr.as_mut_ptr(),
            )
        };
        SpdkError::from_retval(err)?;
        Ok(BdevDesc {
            ptr: unsafe { ptr.assume_init() },
        })
    }

    pub fn get_io_channel(&self) -> Result<IoChannel> {
        let ptr = unsafe { spdk_bdev_get_io_channel(self.ptr) };
        if ptr.is_null() {
            return Err(SpdkError::from(-1));
        }
        Ok(IoChannel { ptr })
    }

    pub fn close(&self) {
        unsafe {
            spdk_bdev_close(self.ptr);
        }
    }

    pub async fn write(&self, offset: usize, length: usize, buf: *mut u8) -> Result<()> {
        let channel = self.get_io_channel().unwrap();
        do_async(|arg| unsafe {
            spdk_bdev_write(
                self.ptr,
                channel.ptr,
                buf as _,
                offset as u64,
                length as u64,
                Some(callback),
                arg,
            );
        })
        .await
    }

    pub async fn read(&self, offset: usize, length: usize, buf: *mut u8) -> Result<()> {
        let channel = self.get_io_channel().unwrap();
        do_async(|arg| unsafe {
            spdk_bdev_read(
                self.ptr,
                channel.ptr,
                buf as _,
                offset as u64,
                length as u64,
                Some(callback),
                arg,
            );
        })
        .await
    }
}

impl Drop for BdevDesc {
    fn drop(&mut self) {
        self.close();
    }
}

extern "C" fn callback(bio: *mut spdk_bdev_io, s: bool, arg: *mut c_void) {
    callback_with(arg, (), s, bio);
}

extern "C" fn callback_with<T>(arg: *mut c_void, bs: T, s: bool, bio: *mut spdk_bdev_io) {
    let complete = unsafe { &mut *(arg as *mut LocalComplete<Result<T>>) };
    let result = if !s { Err(SpdkError::from(-1)) } else { Ok(bs) };
    complete.complete(result);
    unsafe {
        spdk_bdev_free_io(bio);
    }
}

pub async fn do_async<T: Unpin>(f: impl FnOnce(*mut c_void)) -> Result<T> {
    let complete = LocalComplete::<Result<T>>::new();
    futures_lite::pin!(complete);
    f(complete.as_arg());
    complete.await
}
