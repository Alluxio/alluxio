package alluxio.jnifuse.struct;

import java.nio.ByteBuffer;

public class FuseContext extends Struct{

    public final Unsigned32 uid = new Unsigned32();
    public final Unsigned32 gid = new Unsigned32();

    public FuseContext(ByteBuffer buffer) {
        super(buffer);
    }


}
