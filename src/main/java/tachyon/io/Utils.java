package tachyon.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities to do ser/de String, and ByteBuffer
 */
public class Utils {
  public static void writeString(String str, DataOutputStream os) throws IOException {
    if (str == null) {
      os.writeInt(-1);
    } else {
      os.writeInt(str.length());
      os.writeChars(str);
    }
  }

  public static String readString(DataInputStream is) throws IOException {
    int len = is.readInt();

    if (len == -1) {
      return null;
    } else if (len == 0) {
      return "";
    }

    char[] chars = new char[len];
    for (int k = 0; k < len; k ++) {
      chars[k] = is.readChar();
    }
    return new String(chars);

  }

  public static void writeStringList(List<String> list, DataOutputStream os) throws IOException {
    if (list == null) {
      os.writeInt(-1);
      return;
    }
    os.writeInt(list.size());
    for (int k = 0; k < list.size(); k ++) {
      writeString(list.get(k), os);
    }
  }

  public static List<String> readStringList(DataInputStream is) throws IOException {
    int size = is.readInt();

    if (size == -1) {
      return null;
    }

    List<String> ret = new ArrayList<String>(size);
    for (int k = 0; k < size; k ++) {
      ret.add(readString(is));
    }
    return ret;
  }

  public static void writeByteBuffer(ByteBuffer buf, DataOutputStream os) throws IOException {
    if (buf == null) {
      os.writeInt(-1);
      return;
    }
    int len = buf.limit() - buf.position();
    os.writeInt(len);
    os.write(buf.array(), buf.position(), len);
  }

  public static ByteBuffer readByteBuffer(DataInputStream is) throws IOException {
    int len = is.readInt();
    if (len == -1) {
      return null;
    }

    byte[] arr = new byte[len];
    for (int k = 0; k < len ; k ++) {
      arr[k] = is.readByte();
    }

    return ByteBuffer.wrap(arr);
  }

  public static void writeByteBufferList(List<ByteBuffer> list, DataOutputStream os)
      throws IOException {
    if (list == null) {
      os.writeInt(-1);
      return;
    }
    os.writeInt(list.size());
    for (int k = 0; k < list.size(); k ++) {
      writeByteBuffer(list.get(k), os);
    }
  }

  public static List<ByteBuffer> readByteBufferList(DataInputStream is) throws IOException {
    int size = is.readInt();
    if (size == -1) {
      return null;
    }

    List<ByteBuffer> ret = new ArrayList<ByteBuffer>(size);
    for (int k = 0; k < size; k ++) {
      ret.add(readByteBuffer(is));
    }
    return ret;
  }
}
