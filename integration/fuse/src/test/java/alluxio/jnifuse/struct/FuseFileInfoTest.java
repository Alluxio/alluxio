package alluxio.jnifuse.struct;

import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class FuseFileInfoTest {

    @Test
    public void offset() {
        FuseFileInfo jnifi = new FuseFileInfo(ByteBuffer.allocate(256));

        ru.serce.jnrfuse.struct.FuseFileInfo jnrfi = ru.serce.jnrfuse.struct.FuseFileInfo.of(
                Pointer.wrap(Runtime.getSystemRuntime(), 0x0));

        assertEquals(jnrfi.flags.offset(), jnifi.flags.offset());
        assertEquals(jnrfi.fh.offset(), jnifi.fh.offset());

    }

}
