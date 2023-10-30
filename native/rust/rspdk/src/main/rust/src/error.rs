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

use spdk_sys::*;
use std::ffi::CStr;

#[derive(Debug, thiserror::Error)]
#[error("spdk error: {msg}")]
pub struct SpdkError {
    errno: i32,
    msg: String,
}

impl From<i32> for SpdkError {
    fn from(errno: i32) -> Self {
        assert_ne!(errno, 0);
        let cstr = unsafe { spdk_strerror(-errno) };
        SpdkError {
            errno,
            msg: unsafe { CStr::from_ptr(cstr) }.to_str().unwrap().into(),
        }
    }
}

impl SpdkError {
    pub fn from_retval(errno: i32) -> Result<()> {
        if errno == 0 {
            Ok(())
        } else {
            Err(SpdkError::from(errno))
        }
    }
}

pub type Result<T> = std::result::Result<T, SpdkError>;
