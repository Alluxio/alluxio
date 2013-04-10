package tachyon;

import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.client.OpType;
import tachyon.client.OutStream;
import tachyon.client.TachyonClient;
import tachyon.client.TachyonFile;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;

public final class TestUtils {

  /**
   * Create a simple file with length <code>len</code>.
   * @param len
   * @return file id of the new created file.
   * @throws FileAlreadyExistException 
   * @throws InvalidPathException 
   * @throws IOException 
   */
  public static int createSimpleFile(TachyonClient client, String fileName, OpType op, int len)
      throws InvalidPathException, FileAlreadyExistException, IOException {
    int fileId = client.createFile(fileName);
    TachyonFile file = client.getFile(fileId);
    OutStream os = file.createOutStream(op);

    for (int k = 0; k < len; k ++) {
      os.write((byte) k);
    }
    os.close();

    return fileId;
  }

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
