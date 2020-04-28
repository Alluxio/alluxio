package alluxio.jnifuse.struct;

import java.nio.ByteBuffer;

public class FuseFileInfo extends Struct {

    public FuseFileInfo(ByteBuffer buffer) {
        super(buffer);
        flags = new Signed32();
        fh_old = new UnsignedLong();
        writepage = new Signed32();
        pad1 = new Padding(2);
        fh = new u_int64_t();
        lock_owner = new u_int64_t();
    }

    public final Signed32 flags;
    public final UnsignedLong fh_old;
    public final Signed32 writepage;
    public final Padding pad1;
    public final u_int64_t fh;
    public final u_int64_t lock_owner;
}
