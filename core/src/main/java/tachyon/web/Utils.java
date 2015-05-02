package tachyon.web;

public final class Utils {

  public static String convertByteArrayToStringWithoutEscape(byte[] data, int offset, int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = offset; i < length && i < data.length; i ++) {
      sb.append((char) data[i]);
    }
    return sb.toString();
  }
}
