use spdk_sys::*;

#[derive(Debug)]
pub struct DmaBuf {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for DmaBuf {}

impl DmaBuf {
    pub fn alloc(size: usize, align: usize) -> DmaBuf {
        let ptr = unsafe { spdk_dma_malloc(size, align, std::ptr::null_mut()) };
        assert!(!ptr.is_null(), "Failed to malloc");
        DmaBuf {
            ptr: ptr as _,
            len: size,
        }
    }

    pub fn alloc_zeroed(size: usize, align: usize) -> DmaBuf {
        let ptr = unsafe { spdk_dma_zmalloc(size, align, std::ptr::null_mut()) };
        assert!(!ptr.is_null(), "Failed to malloc");
        DmaBuf {
            ptr: ptr as _,
            len: size,
        }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr as _
    }

    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr as _
    }
}

impl AsRef<[u8]> for DmaBuf {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl AsMut<[u8]> for DmaBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for DmaBuf {
    fn drop(&mut self) {
        unsafe { spdk_dma_free(self.ptr as _) }
    }
}
