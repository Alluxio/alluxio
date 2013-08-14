package tachyon.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

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
}
