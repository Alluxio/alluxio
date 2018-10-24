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

package alluxio.util.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A collection of utility functions to read/write primitive values from/to a byte array.
 */
@ThreadSafe
public final class ByteIOUtils {

  private ByteIOUtils() {} // prevent instantiation.

  /**
   * Reads a specific byte value from the input byte array at the given offset.
   *
   * @param buf input byte array
   * @param pos offset into the byte buffer to read
   * @return the byte value read
   */
  public static byte readByte(byte[] buf, int pos) {
    checkBoundary(buf, pos, 1);
    return (byte) (buf[pos] & 0xff);
  }

  /**
   * Reads a specific byte value from the input byte buffer at the given offset.
   *
   * @param buf input byte buffer
   * @param pos offset into the byte buffer to read
   * @return the byte value read
   */
  public static byte readByte(ByteBuffer buf, int pos) {
    return (byte) (buf.get(pos) & 0xff);
  }

  /**
   * Reads a specific short byte value (2 bytes) from the input byte array at the given offset.
   *
   * @param buf input byte buffer
   * @param pos offset into the byte buffer to read
   * @return the short value read
   */
  public static short readShort(byte[] buf, int pos) {
    checkBoundary(buf, pos, 2);
    return (short) (((short) (buf[pos] & 0xff) << 8) | ((short) (buf[pos + 1] & 0xff)));
  }

  /**
   * Reads a specific integer byte value (4 bytes) from the input byte array at the given offset.
   *
   * @param buf input byte array
   * @param pos offset into the byte buffer to read
   * @return the int value read
   */
  public static int readInt(byte[] buf, int pos) {
    checkBoundary(buf, pos, 4);
    return (((buf[pos++] & 0xff) << 24) | ((buf[pos++] & 0xff) << 16)
        | ((buf[pos++] & 0xff) << 8) | (buf[pos] & 0xff));
  }

  /**
   * Reads a specific integer byte value (4 bytes) from the input byte buffer at the given offset.
   *
   * @param buf input byte buffer
   * @param pos offset into the byte buffer to read
   * @return the int value read
   */
  public static int readInt(ByteBuffer buf, int pos) {
    return (((buf.get(pos) & 0xff) << 24) | ((buf.get(pos + 1) & 0xff) << 16)
        | ((buf.get(pos + 2) & 0xff) << 8) | (buf.get(pos + 3) & 0xff));
  }

  /**
   * Reads a specific long byte value (8 bytes) from the input byte array at the given offset.
   *
   * @param buf input byte buffer
   * @param pos offset into the byte buffer to read
   * @return the long value read
   */
  public static long readLong(byte[] buf, int pos) {
    checkBoundary(buf, pos, 8);
    return (((long) (buf[pos++] & 0xff) << 56) | ((long) (buf[pos++] & 0xff) << 48)
        | ((long) (buf[pos++] & 0xff) << 40) | ((long) (buf[pos++] & 0xff) << 32)
        | ((long) (buf[pos++] & 0xff) << 24) | ((long) (buf[pos++] & 0xff) << 16)
        | ((long) (buf[pos++] & 0xff) << 8) | ((long) (buf[pos] & 0xff)));
  }

  /**
   * Writes a single byte value (1 byte) to the output byte array at the given offset.
   *
   * @param buf output byte buffer
   * @param pos offset into the byte buffer to write
   * @param v byte value to write
   */
  public static void writeByte(byte[] buf, int pos, byte v) {
    checkBoundary(buf, pos, 1);
    buf[pos] = v;
  }

  /**
   * Writes a single byte value (1 byte) to the output byte buffer at the given offset.
   *
   * @param buf output byte buffer
   * @param pos offset into the byte buffer to write
   * @param v byte value to write
   */
  public static void writeByte(ByteBuffer buf, int pos, byte v) {
    buf.put(pos, v);
  }

  /**
   * Writes a single byte value (1 byte) to the output stream. This is equivalent of
   * {@link OutputStream#write(int)}.
   *
   * @param out output stream
   * @param v byte value to write
   */
  public static void writeByte(OutputStream out, byte v) throws IOException {
    out.write(v);
  }

  /**
   * Writes a specific short value (2 bytes) to the output byte buffer at the given offset.
   *
   * @param buf output byte buffer
   * @param pos offset into the byte buffer to write
   * @param v short value to write
   */
  public static void writeShort(byte[] buf, int pos, short v) {
    checkBoundary(buf, pos, 2);
    buf[pos++] = (byte) (0xff & (v >> 8));
    buf[pos] = (byte) (0xff & v);
  }

  /**
   * Writes a specific short value (2 bytes) to the output stream.
   *
   * @param out output stream
   * @param v short value to write
   */
  public static void writeShort(OutputStream out, short v) throws IOException {
    out.write((byte) (0xff & (v >> 8)));
    out.write((byte) (0xff & v));
  }

  /**
   * Writes a specific integer value (4 bytes) to the output byte array at the given offset.
   *
   * @param buf output byte array
   * @param pos offset into the byte buffer to write
   * @param v int value to write
   */
  public static void writeInt(byte[] buf, int pos, int v) {
    checkBoundary(buf, pos, 4);
    buf[pos++] = (byte) (0xff & (v >> 24));
    buf[pos++] = (byte) (0xff & (v >> 16));
    buf[pos++] = (byte) (0xff & (v >> 8));
    buf[pos] = (byte) (0xff & v);
  }

  /**
   * Writes a specific integer value (4 bytes) to the output byte buffer at the given offset.
   *
   * @param buf output byte buffer
   * @param pos offset into the byte buffer to write
   * @param v int value to write
   */
  public static void writeInt(ByteBuffer buf, int pos, int v) {
    buf.put(pos++, (byte) (0xff & (v >> 24)));
    buf.put(pos++, (byte) (0xff & (v >> 16)));
    buf.put(pos++, (byte) (0xff & (v >> 8)));
    buf.put(pos, (byte) (0xff & v));
  }

  /**
   * Writes a specific integer value (4 bytes) to the output stream.
   *
   * @param out output stream
   * @param v integer value to write
   */
  public static void writeInt(OutputStream out, int v) throws IOException {
    out.write((byte) (0xff & (v >> 24)));
    out.write((byte) (0xff & (v >> 16)));
    out.write((byte) (0xff & (v >> 8)));
    out.write((byte) (0xff & v));
  }

  /**
   * Writes a specific long value (8 bytes) to the output byte array at the given offset.
   *
   * @param buf output byte array
   * @param pos offset into the byte buffer to write
   * @param v long value to write
   */
  public static void writeLong(byte[] buf, int pos, long v) {
    checkBoundary(buf, pos, 8);
    buf[pos++] = (byte) (0xff & (v >> 56));
    buf[pos++] = (byte) (0xff & (v >> 48));
    buf[pos++] = (byte) (0xff & (v >> 40));
    buf[pos++] = (byte) (0xff & (v >> 32));
    buf[pos++] = (byte) (0xff & (v >> 24));
    buf[pos++] = (byte) (0xff & (v >> 16));
    buf[pos++] = (byte) (0xff & (v >> 8));
    buf[pos] = (byte) (0xff & v);
  }

  /**
   * Writes a specific long value (8 bytes) to the output stream.
   *
   * @param out output stream
   * @param v long value to write
   */
  public static void writeLong(OutputStream out, long v) throws IOException {
    out.write((byte) (0xff & (v >> 56)));
    out.write((byte) (0xff & (v >> 48)));
    out.write((byte) (0xff & (v >> 40)));
    out.write((byte) (0xff & (v >> 32)));
    out.write((byte) (0xff & (v >> 24)));
    out.write((byte) (0xff & (v >> 16)));
    out.write((byte) (0xff & (v >> 8)));
    out.write((byte) (0xff & v));
  }

  /**
   * Ensures that the given buffer contains at least the given number of bytes after the given
   * offset.
   *
   * @param buf input byte array
   * @param pos position in the byte array to start writing
   * @param len length of data to write from the given position
   */
  private static void checkBoundary(byte[] buf, int pos, int len) {
    if (pos + len > buf.length) {
      throw new ArrayIndexOutOfBoundsException();
    }
  }
}
