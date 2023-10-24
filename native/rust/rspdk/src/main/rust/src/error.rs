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
