package tachyon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;

import tachyon.thrift.InvalidPathException;

/**
 * Common utilities shared by all components in Tachyon.
 */
public final class CommonUtils {
  private static final Logger LOG = Logger.getLogger("");

  private CommonUtils () {
  }

  public static String cleanPath(String path) throws IOException {
    if (path == null || path.isEmpty()) {
      throw new IOException("Path (" + path + ") is invalid.");
    }
    while (path.endsWith("/") && path.length() > 1) {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }

  public static ByteBuffer cloneByteBuffer(ByteBuffer buf) {
    ByteBuffer ret = ByteBuffer.allocate(buf.limit() - buf.position());
    ret.put(buf);
    ret.flip();
    buf.flip();
    return ret;
  }

  public static List<ByteBuffer> cloneByteBufferList(List<ByteBuffer> source) {
    List<ByteBuffer> ret = new ArrayList<ByteBuffer>(source.size());
    for (int k = 0; k < source.size(); k ++) {
      ret.add(cloneByteBuffer(source.get(k)));
    }
    return ret;
  }

  public static String convertByteArrayToString(byte[] data) {
    StringBuilder sb = new StringBuilder(data.length);
    for (int i = 0; i < data.length; ++ i) {
      if (data[i] < 128) {
        sb.append((char) data[i]);
      } else {
        return null;
      }
    }
    return StringEscapeUtils.escapeHtml3(sb.toString()).replace("\n", "<br/>");
  }

  public static String convertMsToClockTime(long Millis) {
    long days = Millis / Constants.DAY_MS;
    long hours = (Millis % Constants.DAY_MS) / Constants.HOUR_MS;
    long mins = (Millis % Constants.HOUR_MS) / Constants.MINUTE_MS;
    long secs = (Millis % Constants.MINUTE_MS) / Constants.SECOND_MS;

    return String.format("%d day(s), %d hour(s), %d minute(s), and %d second(s)",
        days, hours, mins, secs);
  }

  public static String convertMsToShortClockTime(long Millis) {
    long days = Millis / Constants.DAY_MS;
    long hours = (Millis % Constants.DAY_MS) / Constants.HOUR_MS;
    long mins = (Millis % Constants.HOUR_MS) / Constants.MINUTE_MS;
    long secs = (Millis % Constants.MINUTE_MS) / Constants.SECOND_MS;

    return String.format("%d d, %d h, %d m, and %d s", days, hours, mins, secs);
  }

  public static String convertMsToDate(long Millis) {
    DateFormat formatter = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss:SSS");
    return formatter.format(new Date(Millis));
  }

  public static String convertMsToSimpleDate(long Millis) {
    DateFormat formatter = new SimpleDateFormat("MM-dd-yyyy");
    return formatter.format(new Date(Millis));
  }

  public static ByteBuffer generateNewByteBufferFromThriftRPCResults(ByteBuffer data) {
    // TODO this is a trick to fix the issue in thrift. Change the code to use metadata directly
    // when thrift fixes the issue.
    ByteBuffer correctData = ByteBuffer.allocate(data.limit() - data.position());
    correctData.put(data);
    correctData.flip();
    return correctData;
  }

  public static long getCurrentMs() {
    return System.currentTimeMillis();
  }

  public static long getCurrentNs() {
    return System.nanoTime();
  }

  public static long getBlockIdFromFileName(String name) {
    long fileId;
    try {
      fileId = Long.parseLong(name);
    } catch (Exception e) {
      throw new IllegalArgumentException("Wrong file name: " + name);
    }
    return fileId;
  }

  public static int getMB(int bytes) {
    return bytes / Constants.MB;
  }

  public static String getSizeFromBytes(long bytes) {
    double ret = bytes;
    if (ret <= 1024 * 5) {
      return String.format("%.2f B", ret); 
    }
    ret /= 1024;
    if (ret <= 1024 * 5) {
      return String.format("%.2f KB", ret);
    }
    ret /= 1024;
    if (ret <= 1024 * 5) {
      return String.format("%.2f MB", ret);
    }
    ret /= 1024;
    return String.format("%.2f GB", ret);
  }

  public static void illegalArgumentException(String msg) {
    throw new IllegalArgumentException(msg);
  }

  public static void illegalArgumentException(Exception e) {
    LOG.error(e.getMessage(), e);
    throw new IllegalArgumentException(e);
  }

  public static <T> String listToString(List<T> list) {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < list.size(); k ++) {
      sb.append(list.get(k)).append(" ");
    }
    return sb.toString();
  }

  public static String parametersToString(Object ... objs) {
    StringBuilder sb = new StringBuilder("(");
    for (int k = 0; k < objs.length; k ++) {
      if (k != 0) {
        sb.append(", ");
      }
      sb.append(objs[k].toString());
    }
    sb.append(")");
    return sb.toString();
  }

  public static long parseMemorySize(String memorySize) {
    String ori = memorySize;
    String end = "";
    int tIndex = memorySize.length() - 1;
    while (tIndex >= 0) {
      if (memorySize.charAt(tIndex) > '9' || memorySize.charAt(tIndex) < '0') {
        end = memorySize.charAt(tIndex) + end;
      } else {
        break;
      }
      tIndex --;
    }
    memorySize = memorySize.substring(0, tIndex + 1);
    long ret = Long.parseLong(memorySize);
    end = end.toLowerCase();
    if (end.equals("") || end.equals("b")) {
      return ret;
    } else if (end.equals("kb")) {
      return ret * Constants.KB;
    } else if (end.equals("mb")) {
      return ret * Constants.MB;
    } else if (end.equals("gb")) {
      return ret * Constants.GB;
    }
    runtimeException("Fail to parse " + ori + " as memory size");
    return -1;
  }

  public static void printByteBuffer(Logger LOG, ByteBuffer buf) {
    String tmp = "";
    for (int k = 0; k < buf.limit() / 4; k ++) {
      tmp += buf.getInt() + " ";
    }

    LOG.info(tmp);
  }

  public static void printTimeTakenMs(long startTimeMs, Logger logger, String message) {
    logger.info(message + " took " + (getCurrentMs() - startTimeMs) + " ms.");
  }

  public static void printTimeTakenNs(long startTimeNs, Logger logger, String message) {
    logger.info(message + " took " + (getCurrentNs() - startTimeNs) + " ns.");
  }

  public static void runtimeException(String msg) {
    throw new RuntimeException(msg);
  }

  public static void runtimeException(Exception e) {
    LOG.error(e.getMessage(), e);
    throw new RuntimeException(e);
  }

  public static void sleepMs(Logger logger, long timeMs) {
    try {
      Thread.sleep(timeMs);
    } catch (InterruptedException e) {
      logger.warn(e.getMessage(), e);
    }
  }

  public static String[] toStringArray(ArrayList<String> src) {
    String[] ret = new String[src.size()];
    return src.toArray(ret);
  }

  public static void tempoaryLog(String msg) {
    LOG.info("Temporary Log ============================== " + msg);
  }

  public static void validatePath(String path) throws InvalidPathException {
    if (path == null || !path.startsWith(Constants.PATH_SEPARATOR) || 
        (path.length() > 1 && path.endsWith(Constants.PATH_SEPARATOR)) ||
        path.contains(" ")) {
      throw new InvalidPathException("Path " + path + " is invalid.");
    }
  }
}