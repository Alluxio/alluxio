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

use crate::complete::LocalComplete;
use spdk_sys::*;
use std::{
    cell::RefCell,
    ffi::{c_void, CString},
    future::Future,
    mem::MaybeUninit,
    os::raw::c_int,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

pub struct SpdkConsumer {
    opts: spdk_app_opts,
}

impl SpdkConsumer {
    pub fn new() -> Self {
        let mut opts = MaybeUninit::uninit();
        unsafe {
            spdk_app_opts_init(opts.as_mut_ptr(), std::mem::size_of::<spdk_app_opts>());
        }
        SpdkConsumer {
            opts: unsafe { opts.assume_init() },
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.opts.name = CString::new(name).unwrap().into_raw();
        self
    }

    pub fn config_file(mut self, config_file: &str) -> Self {
        self.opts.json_config_file = CString::new(config_file).unwrap().into_raw();
        self
    }

    pub fn block_on<F: Future>(mut self, future: F) -> F::Output {
        extern "C" fn start_fn<F: Future>(arg: *mut c_void) {
            let (future, output_ptr) = unsafe { *Box::from_raw(arg as *mut (F, *mut F::Output)) };
            spawn_internal(future, output_ptr);
        }
        let mut output = MaybeUninit::uninit();
        unsafe {
            let err = spdk_app_start(
                &mut self.opts,
                Some(start_fn::<F>),
                Box::into_raw(Box::new((future, output.as_mut_ptr()))) as *mut c_void,
            );
            assert_eq!(err, 0);
            spdk_app_fini();
            output.assume_init()
        }
    }
}

pub fn spawn<'a, F: Future + 'a>(future: F) -> JoinHandle<F> {
    spawn_internal(future, std::ptr::null_mut())
}

struct Task<F: Future> {
    future: F,
    poller: *mut spdk_poller,
    waker: Waker,
    output_ptr: *mut F::Output,
    output: LocalComplete<F::Output>,
}

pub struct JoinHandle<F: Future> {
    task: Rc<RefCell<Task<F>>>,
}

impl<F: Future<Output = T>, T: Unpin> Future for JoinHandle<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut task = self.task.borrow_mut();
        Pin::new(&mut task.output).poll(cx)
    }
}

fn spawn_internal<F: Future>(future: F, output_ptr: *mut F::Output) -> JoinHandle<F> {
    extern "C" fn poller_wrapper<F: Future>(cell_ptr: *mut c_void) -> c_int {
        let cell_ptr = cell_ptr as *const RefCell<Task<F>>;
        let task = &mut *unsafe { &*cell_ptr }.borrow_mut();
        let mut context = Context::from_waker(&task.waker);
        let future = unsafe { Pin::new_unchecked(&mut task.future) };
        match future.poll(&mut context) {
            Poll::Pending => unsafe { spdk_poller_pause(task.poller) },
            Poll::Ready(output) => unsafe {
                if task.output_ptr.is_null() {
                    task.output.complete(output);
                } else {
                    task.output_ptr.write(output);
                }
                spdk_poller_unregister(&mut task.poller);
                std::thread::sleep(std::time::Duration::from_secs(1));
                Rc::from_raw(cell_ptr);
            },
        }
        1
    }
    let task = Rc::new(RefCell::new(Task {
        future,
        poller: std::ptr::null_mut(),
        waker: unsafe { poller_waker(std::ptr::null_mut()) },
        output_ptr,
        output: LocalComplete::new(),
    }));
    let arg = Rc::into_raw(task.clone());
    let poller = unsafe { spdk_poller_register(Some(poller_wrapper::<F>), arg as _, 0) };
    assert!(!poller.is_null());
    {
        let mut task0 = task.borrow_mut();
        task0.poller = poller;
        task0.waker = unsafe { poller_waker(poller) };
    }
    JoinHandle { task }
}

unsafe fn poller_waker(poller: *mut spdk_poller) -> Waker {
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        |data| RawWaker::new(data, &VTABLE),                 // clone
        |poller| unsafe { spdk_poller_resume(poller as _) }, // wake
        |poller| unsafe { spdk_poller_resume(poller as _) }, // wake_by_ref
        |_| {},                                              // drop
    );
    Waker::from_raw(RawWaker::new(poller as _, &VTABLE))
}
