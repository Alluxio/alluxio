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

use std::{
    cell::UnsafeCell,
    ffi::c_void,
    future::Future,
    pin::Pin,
    task::{Poll, Waker},
};

#[derive(Default, Debug)]
pub struct LocalComplete<T> {
    waker: Option<Waker>,
    value: Option<T>,
    // mark !Send
    _marker: UnsafeCell<()>,
}

impl<T> LocalComplete<T> {
    pub const fn new() -> Self {
        LocalComplete {
            waker: None,
            value: None,
            _marker: UnsafeCell::new(()),
        }
    }

    pub fn complete(&mut self, result: T) {
        self.value = Some(result);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    pub fn as_arg(&mut self) -> *mut c_void {
        self as *mut Self as _
    }
}

impl<T: Unpin> Future for LocalComplete<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Self::Output> {
        if let Some(v) = self.value.take() {
            return Poll::Ready(v);
        }
        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
