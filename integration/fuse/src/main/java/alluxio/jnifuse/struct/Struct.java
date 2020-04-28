package alluxio.jnifuse.struct;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public abstract class Struct {
    static final Charset ASCII = StandardCharsets.US_ASCII;
    static final Charset UTF8 = StandardCharsets.UTF_8;

    public ByteBuffer buffer;

    static final class Info {
        int _offset = 0;

        private static int align(int offset, int align) {
            return (offset + align - 1) & ~(align - 1);
        }

        protected final int addField(int sizeBits, int alignBits) {
            final int alignment = alignBits >> 3;
            final int offset = align(this._offset, alignment);
            this._offset += sizeBits >> 3;
            return offset;
        }
    }

    final Info _info;

    protected Struct(ByteBuffer bb) {
        this._info = new Info();
        this.buffer = bb;
        bb.order(ByteOrder.LITTLE_ENDIAN);
    }

    protected abstract class Member {
        abstract int offset();
    }

    public abstract class NumberField extends Member {
        private final int offset;

        protected NumberField() {
            this.offset = _info.addField(getSize() * 8, getAlignment() * 8);
        }

        public final int offset() {
            return offset;
        }

        protected abstract int getSize();

        protected abstract int getAlignment();
    }

    public class Signed16 extends NumberField {
        @Override
        protected int getSize() {
            return 2;
        }

        @Override
        protected int getAlignment() {
            return 2;
        }

        public final short get() {
            return buffer.getShort(offset());
        }

        public final void set(short value) {
            buffer.putShort(offset(), value);
        }
    }

    public class Unsigned16 extends NumberField {

        @Override
        protected int getSize() {
            return 2;
        }

        @Override
        protected int getAlignment() {
            return 2;
        }

        public final int get() {
            int value = buffer.getShort(offset());
            return value < 0 ? (int) ((value & 0x7FFF) + 0x8000) : value;
        }

        public final void set(int value) {
            buffer.putShort(offset(), (short) value);
        }
    }

    public class Signed32 extends NumberField {

        @Override
        protected int getSize() {
            return 4;
        }

        @Override
        protected int getAlignment() {
            return 4;
        }

        public final void set(int value) {
            buffer.putInt(offset(), value);
        }

        public final int get() {
            return buffer.getInt(offset());
        }
    }

    public class Unsigned32 extends NumberField {
        @Override
        protected int getSize() {
            return 4;
        }

        @Override
        protected int getAlignment() {
            return 4;
        }

        public final long get() {
            long value = buffer.getInt(offset());
            return value < 0 ? (long) ((value & 0x7FFFFFFFL) + 0x80000000L) : value;
        }

        public final void set(long value) {
            buffer.putInt(offset(), (int) value);
        }
    }

    public class Signed64 extends NumberField {

        @Override
        protected int getSize() {
            return 8;
        }

        @Override
        protected int getAlignment() {
            return 8;
        }

        public final long get() {
            return buffer.getLong(offset());
        }

        public final void set(long value) {
            buffer.putLong(offset(), value);
        }
    }

    public class Unsigned64 extends NumberField {

        @Override
        protected int getSize() {
            return 8;
        }

        @Override
        protected int getAlignment() {
            return 8;
        }

        public final long get() {
            return buffer.getLong(offset());
        }

        public final void set(long value) {
            buffer.putLong(offset(), value);
        }
    }

    public class SignedLong extends NumberField {

        @Override
        protected int getSize() {
            return 8;
        }

        @Override
        protected int getAlignment() {
            return 8;
        }

        public final long get() {
            return buffer.getLong(offset());
        }

        public final void set(long value) {
            buffer.putLong(offset(), value);
        }
    }

    public class UnsignedLong extends NumberField {

        @Override
        protected int getSize() {
            return 8;
        }

        @Override
        protected int getAlignment() {
            return 8;
        }

        public final long get() {
            return buffer.getLong(offset());
        }

        public final void set(long value) {
            buffer.putLong(offset(), value);
        }
    }

    public final class u_int64_t extends NumberField {

        @Override
        protected int getSize() {
            return 8;
        }

        @Override
        protected int getAlignment() {
            return 8;
        }

        public final long get() {
            return buffer.getLong(offset());
        }

        public final void set(long value) {
            buffer.putLong(offset(), value);
        }
    }

    protected abstract class AbstrctMember extends Member {
        private final int offset;

        protected AbstrctMember(int size) {
            this.offset = _info.addField(size * 8, 8);
        }

        @Override
        public final int offset() {
            return offset;
        }
    }

    public final class Padding extends AbstrctMember {
        public Padding(int size) {
            super(size);
        }
    }
}
