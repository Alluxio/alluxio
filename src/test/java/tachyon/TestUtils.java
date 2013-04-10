package tachyon;

import java.nio.ByteBuffer;

public final class TestUtils {
  public static byte[] getIncreasingByteArray(int len) {
    byte[] ret = new byte[len];
    for (int k = 0; k < len; k ++) {
      ret[k] = (byte) k;
    }
    return ret;
  }

  public static boolean equalIncreasingByteArray(int len, byte[] arr) {
    if (arr == null || arr.length != len) {
      return false;
    }
    for (int k = 0; k < len; k ++) {
      if (arr[k] != (byte) k) {
        return false;
      }
    }
    return true;
  }

  public static ByteBuffer getIncreasingByteBuffer(int len) {
    ByteBuffer ret = ByteBuffer.allocate(len);
    for (int k = 0; k < len; k ++) {
      ret.put((byte) k);
    }
    ret.flip();
    return ret;
  }

  public static boolean equalIncreasingByteBuffer(int len, ByteBuffer buf) {
    if (buf == null || buf.capacity() != len || buf.position() != 0) {
      return false;
    }
    for (int k = 0; k < len; k ++) {
      byte t = buf.get();
      System.out.println(k + " : " + t + " " + (byte) k);
      if (t != (byte) k)
        return false;
    }
    System.out.println("Good");
    return true;
  }

  public static ByteBuffer getIncreasingIntBuffer(int len) {
    ByteBuffer ret = ByteBuffer.allocate(len * 4);
    for (int k = 0; k < len; k ++) {
      ret.putInt(k);
    }
    ret.flip();
    return ret;
  }

  public static boolean equalIncreasingIntBuffer(int len, ByteBuffer buf) {
    if (buf == null || buf.capacity() != len * 4 || buf.position() != 0) {
      return false;
    }
    for (int k = 0; k < len; k ++) {
      if (buf.getInt() != k)
        return false;
    }
    return true;
  }
}
