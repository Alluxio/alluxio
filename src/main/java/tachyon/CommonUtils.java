package tachyon;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class shared by all components of the system.
 * 
 * @author haoyuan
 */
public class CommonUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CommonUtils.class);

  public static void sleep(long timeMs) {
    try {
      Thread.sleep(timeMs);
    } catch (InterruptedException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public static long getCurrentMs() {
    return System.currentTimeMillis();
  }

  public static long getCurrentNs() {
    return System.nanoTime();
  }

  public static String getLocalFilePath(String localFolder, long bigId) {
    return localFolder + "/" + computeDatasetIdFromBigId(bigId) + "-" 
        + computePartitionIdFromBigId(bigId);
  }

  public static void printTimeTakenMs(long startTimeMs, Logger logger, String message) {
    logger.info(message + " took " + (getCurrentMs() - startTimeMs) + " ms.");
  }

  public static void printTimeTakenNs(long startTimeNs, Logger logger, String message) {
    logger.info(message + " took " + (getCurrentNs() - startTimeNs) + " ns.");
  }

  public static void printByteBuffer(Logger LOG, ByteBuffer buf) {
    String tmp = "";
    for (int k = 0; k < buf.limit() / 4; k ++) {
      tmp += buf.getInt() + " ";
    }

    LOG.info(tmp);
  }

  public static void runtimeException(String msg) {
    throw new RuntimeException(msg);
  }

  public static void runtimeException(Exception e) {
    LOG.error(e.getMessage(), e);
    throw new RuntimeException(e);
  }

  public static void illegalArgumentException(String msg) {
    throw new IllegalArgumentException(msg);
  }

  public static void illegalArgumentException(Exception e) {
    LOG.error(e.getMessage(), e);
    throw new IllegalArgumentException(e);
  }

  public static int getDatasetIdFromFileName(String name) {
    String[] p = name.split("-");
    if (p.length != 2) {
      throw new IllegalArgumentException("Wrong file name: " + name);
    }
    int datasetId;
    try {
      datasetId = Integer.parseInt(p[0]);
    } catch (Exception e) {
      throw new IllegalArgumentException("Wrong file name: " + name);
    }
    return datasetId;
  }

  public static int getPartitionIdFromFileName(String name) {
    String[] p = name.split("-");
    if (p.length != 2) {
      throw new IllegalArgumentException("Wrong file name: " + name);
    }
    int pId;
    try {
      pId = Integer.parseInt(p[1]);
    } catch (Exception e) {
      throw new IllegalArgumentException("Wrong file name: " + name);
    }
    return pId;
  }

  public static long generateBigId(int datasetId, int partitionId) {
    long ret = datasetId;
    ret = (ret << 32) + partitionId; 
    return ret;
  }

  public static int computeDatasetIdFromBigId(long bigId) {
    return (int)(bigId >> 32);
  }

  public static int computePartitionIdFromBigId(long bigId) {
    return (int)(bigId % Config.TWO_32);
  }

  public static String convertMillis(long Millis) {
    return String.format("%d hour(s), %d minute(s), and %d second(s)",
        Millis / (1000L * 60 * 60), (Millis % (1000L * 60 * 60)) / (1000 * 60),
        (Millis % (1000L * 60)) / 1000);
  }

  public static String convertMillisToDate(long Millis) {
    DateFormat formatter = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss:SSS");
    return formatter.format(new Date(Millis));
  }

  public static int getKB(int bytes) {
    return bytes / 1024;
  }

  public static long getKB(long bytes) {
    return bytes / 1024;
  }

  public static int getMB(int bytes) {
    return bytes / 1024 / 1024;
  }

  public static long getMB(long bytes) {
    return bytes / 1024 / 1024;
  }

  public static byte[] getMd5(byte[] data) {
    return DigestUtils.md5(data);
  }

  public static String getMd5Hex(byte[] data) {
    return DigestUtils.md5Hex(data);
  }

  public static String getMd5Hex(String fileName) {
    String ret = null;
    try {
      FileInputStream fis = new FileInputStream(fileName);
      ret = DigestUtils.md5Hex(fis);
    } catch (FileNotFoundException e) {
      runtimeException(e);
    } catch (IOException e) {
      runtimeException(e);
    }
    return ret;
  }

  public static String getSizeFromBytes(long bytes) {
    double ret = bytes;
    if (ret <= 1024 * 5) {
      return String.format("%.2f Bytes", ret); 
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

  public static String getCurrentMemStatsInBytes() {
    Runtime runtime = Runtime.getRuntime();
    StringBuilder sb = new StringBuilder();
    sb.append(" MaxMemory=").append((runtime.maxMemory())).append(" bytes");
    sb.append(" TotalMemory=").append((runtime.totalMemory())).append(" bytes");
    sb.append(" FreeMemory=").append((runtime.freeMemory())).append(" bytes");
    sb.append(" UsedMemory=").append((runtime.totalMemory() - runtime.freeMemory())).append(" bytes");
    return sb.toString();
  }

  public static String getCurrentMemStats() {
    Runtime runtime = Runtime.getRuntime();
    StringBuilder sb = new StringBuilder();
    sb.append(" MaxMemory=").append(getSizeFromBytes(runtime.maxMemory()));
    sb.append(" TotalMemory=").append(getSizeFromBytes(runtime.totalMemory()));
    sb.append(" FreeMemory=").append(getSizeFromBytes(runtime.freeMemory()));
    sb.append(" UsedMemory=").append(getSizeFromBytes(runtime.totalMemory() - runtime.freeMemory()));
    return sb.toString();
  }

  public static String getCurrentMemStatsAfterGCs() {
    for (int k = 0; k < 10; k ++) {
      System.gc();
    }
    return getCurrentMemStats();
  }

  public static String cleanPath(String path) {
    while (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }

  public static String[] toStringArray(ArrayList<String> src) {
    String[] ret = new String[src.size()];
    return src.toArray(ret);
  }
}