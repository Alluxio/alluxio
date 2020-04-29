/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

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

    public final int intValue() {
      return (int) get();
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

    public final int intValue() {
      return (int) get();
    }

    public final long longValue() {
      return get();
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

    public final int intValue() {
      return (int) get();
    }

    public final long longValue() {
      return get();
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

  public final class Timespec extends Member {
    public final SignedLong tv_sec;
    public final SignedLong tv_nsec;
    public final int offset;

    protected Timespec() {
      // TODO: this may cause error
      tv_sec = new SignedLong();
      tv_nsec = new SignedLong();
      offset = tv_sec.offset();
    }

    @Override
    int offset() {
      return offset;
    }

    protected int getSize() {
      return tv_sec.getSize() + tv_nsec.getSize();
    }

    protected int getAlignment() {
      return Math.max(tv_sec.getAlignment(), tv_nsec.getAlignment());
    }
  }
}
