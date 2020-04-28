package alluxio.jnifuse.struct;

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class StatvfsTest {
    @Test
    public void offset() {
        Statvfs jni = new Statvfs(ByteBuffer.allocate(256));

        ru.serce.jnrfuse.struct.Statvfs jnr = ru.serce.jnrfuse.struct.Statvfs.of(
                Pointer.wrap(Runtime.getSystemRuntime(), 0x0));

        assertEquals(jnr.f_bsize.offset(), jni.f_bsize.offset());
        assertEquals(jnr.f_frsize.offset(), jni.f_frsize.offset());
        assertEquals(jnr.f_frsize.offset(), jni.f_frsize.offset());
        assertEquals(jnr.f_bfree.offset(), jni.f_bfree.offset());
        assertEquals(jnr.f_bavail.offset(), jni.f_bavail.offset());
        assertEquals(jnr.f_files.offset(), jni.f_files.offset());
        assertEquals(jnr.f_ffree.offset(), jni.f_ffree.offset());
        assertEquals(jnr.f_favail.offset(), jni.f_favail.offset());
        assertEquals(jnr.f_fsid.offset(), jni.f_fsid.offset());
        assertEquals(jnr.f_flag.offset(), jni.f_flag.offset());
        assertEquals(jnr.f_namemax.offset(), jni.f_namemax.offset());
    }
}
