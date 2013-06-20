package tachyon;

import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.client.OutStream;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.InvalidPathException;

public final class TestUtils {
  /**
   * Create a simple file with <code>len</code> bytes.
   * @param tfs
   * @param fileName
   * @param op
   * @param len
   * @return created file id.
   * @throws InvalidPathException
   * @throws FileAlreadyExistException
   * @throws IOException
   */
  public static int createByteFile(TachyonFS tfs, String fileName, WriteType op, int len)
      throws IOException {
    int fileId = tfs.createFile(fileName);
    TachyonFile file = tfs.getFile(fileId);
    OutStream os = file.getOutStream(op);

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

  public static ByteBuffer getIncreasingIntBuffer(int len) {
    ByteBuffer ret = ByteBuffer.allocate(len * 4);
    for (int k = 0; k < len; k ++) {
      ret.putInt(k);
    }
    ret.flip();
    return ret;
  }
}
