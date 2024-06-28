package alluxio.shuttle.message;

import io.netty.buffer.ByteBuf;

public abstract class Message {
    public final static byte VERSION = 1;
    public final static int CONTROL_LEN = 2 * Byte.BYTES + Long.BYTES + Integer.BYTES;
    public final static int PROTOCOL_LEN = 2 * Integer.BYTES;
    public final static int END_SEQ_ID = -100;
    public final static int INIT_SEQ_ID = -99;

    public abstract ByteBuf encode();
    public abstract long getReqId();
    public abstract boolean isLastReq();
    public abstract void releaseBody();
    public abstract boolean isError();
    public abstract ByteBuf getNettyBody();

    public enum Status {
        HEARTBEAT((byte)20),
        OPEN((byte)21),
        RUNNING((byte)22),

        SUCCESS((byte)23),
        COMPLETE((byte)24),
        CANCEL((byte)25),
        ERROR((byte)26);

        public final byte code;

        Status(byte code) {
            this.code = code;
        }

        private static final int FIRST_CODE = values()[0].code;

        public static Status valueOf(byte code) {
            final int i = (code & 0xff) - FIRST_CODE;
            return i < 0 || i >= values().length ? null : values()[i];
        }

        public static Status read(ByteBuf buf) {
            return valueOf(buf.readByte());
        }
    }
}
