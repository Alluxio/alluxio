package alluxio.jnifuse.struct;

import alluxio.proto.journal.File;
import jnr.ffi.Runtime;
import org.junit.Test;
import ru.serce.jnrfuse.LibFuse;
import ru.serce.jnrfuse.struct.FuseContext;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

public class FileStatTest {

    @Test
    public void offset() {
        // allocate an enough large memory for jnistat
        FileStat jnistat = new FileStat(ByteBuffer.allocate(256));
        ru.serce.jnrfuse.struct.FileStat jnrstat = new ru.serce.jnrfuse.struct.FileStat(Runtime.getSystemRuntime());

        assertEquals(jnrstat.st_dev.offset(), jnistat.st_dev.offset());
        assertEquals(jnrstat.st_ino.offset(), jnistat.st_ino.offset());
        assertEquals(jnrstat.st_nlink.offset(), jnistat.st_nlink.offset());
        assertEquals(jnrstat.st_mode.offset(), jnistat.st_mode.offset());
        assertEquals(jnrstat.st_uid.offset(), jnistat.st_uid.offset());
        assertEquals(jnrstat.st_gid.offset(), jnistat.st_gid.offset());
        assertEquals(jnrstat.st_rdev.offset(), jnistat.st_rdev.offset());
        assertEquals(jnrstat.st_size.offset(), jnistat.st_size.offset());
        assertEquals(jnrstat.st_blksize.offset(), jnistat.st_blksize.offset());
        assertEquals(jnrstat.st_blocks.offset(), jnistat.st_blocks.offset());
    }

    @Test
    public void dataConsistency() {
        FileStat stat = new FileStat(ByteBuffer.allocateDirect(256));
        int mode = FileStat.ALL_READ | FileStat.ALL_WRITE | FileStat.S_IFDIR;
        long size = 0x123456789888721L;
        stat.st_mode.set(mode);
        stat.st_size.set(size);
        assertEquals(mode, stat.st_mode.get());
        assertEquals(size, stat.st_size.get());

        ByteBuffer buf = stat.buffer;
        assertEquals(mode, buf.getInt(0x18));
        assertEquals(size, buf.getLong(0x30));
    }
}
