package alluxio.jnifuse.struct;

import jnr.ffi.Runtime;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Fuse2FuseFileInfo extends ru.serce.jnrfuse.struct.FuseFileInfo implements FuseFileInfo {
  private final ByteBuffer buffer;

  public Fuse2FuseFileInfo(Runtime runtime, ByteBuffer buffer) {
    super(runtime);
    this.buffer = buffer;
    // depends on the arch
    this.buffer.order(ByteOrder.LITTLE_ENDIAN);
  }

  @Override
  public u_int64_t fh() {
    return this.fh;
  }

  @Override
  public Signed32 flags() {
    return this.flags;
  }

}
