/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.UnderFileSystem;
import tachyon.thrift.InvalidPathException;

/**
 * Common utilities shared by all components in Tachyon.
 */
public final class CommonUtils {
  private static final Logger LOG = Logger.getLogger("");

  /**
   * Change local file's permission.
   * 
   * @param filePath
   *          that will change permission
   * @param perms
   *          the permission, e.g. "775"
   * @throws IOException
   */
  public static void changeLocalFilePermission(String filePath, String perms) throws IOException {
    List<String> commands = new ArrayList<String>();
    commands.add("/bin/chmod");
    commands.add(perms);
    File file = new File(filePath);
    commands.add(file.getAbsolutePath());

    try {
      ProcessBuilder builder = new ProcessBuilder(commands);
      Process process = builder.start();

      redirectStreamAsync(process.getInputStream(), System.out);
      redirectStreamAsync(process.getErrorStream(), System.err);

      process.waitFor();

      if (process.exitValue() != 0) {
        throw new IOException("Can not change the file " + file.getAbsolutePath()
            + " 's permission to be " + perms);
      }
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
      throw new IOException(e);
    }
  }

  public static void changeLocalFileToFullPermission(String filePath) throws IOException {
    changeLocalFilePermission(filePath, "777");
  }

  public static String cleanPath(String path) throws IOException {
    if (path == null || path.isEmpty()) {
      throw new IOException("Path (" + path + ") is invalid.");
    }
    while (path.endsWith(Constants.PATH_SEPARATOR) && path.length() > 1) {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }

  public static ByteBuffer cloneByteBuffer(ByteBuffer buf) {
    ByteBuffer ret = ByteBuffer.allocate(buf.limit() - buf.position());
    ret.put(buf.array(), buf.position(), buf.limit() - buf.position());
    ret.flip();
    return ret;
  }

  public static List<ByteBuffer> cloneByteBufferList(List<ByteBuffer> source) {
    List<ByteBuffer> ret = new ArrayList<ByteBuffer>(source.size());
    for (int k = 0; k < source.size(); k ++) {
      ret.add(cloneByteBuffer(source.get(k)));
    }
    return ret;
  }

  public static String convertByteArrayToStringWithoutEscape(byte[] data) {
    StringBuilder sb = new StringBuilder(data.length);
    for (int i = 0; i < data.length; i ++) {
      if (data[i] < 128) {
        sb.append((char) data[i]);
      } else {
        return null;
      }
    }
    return sb.toString();
  }

  public static String convertMsToClockTime(long Millis) {
    long days = Millis / Constants.DAY_MS;
    long hours = (Millis % Constants.DAY_MS) / Constants.HOUR_MS;
    long mins = (Millis % Constants.HOUR_MS) / Constants.MINUTE_MS;
    long secs = (Millis % Constants.MINUTE_MS) / Constants.SECOND_MS;

    return String.format("%d day(s), %d hour(s), %d minute(s), and %d second(s)", days, hours,
        mins, secs);
  }

  public static String convertMsToDate(long Millis) {
    DateFormat formatter = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss:SSS");
    return formatter.format(new Date(Millis));
  }

  public static String convertMsToShortClockTime(long Millis) {
    long days = Millis / Constants.DAY_MS;
    long hours = (Millis % Constants.DAY_MS) / Constants.HOUR_MS;
    long mins = (Millis % Constants.HOUR_MS) / Constants.MINUTE_MS;
    long secs = (Millis % Constants.MINUTE_MS) / Constants.SECOND_MS;

    return String.format("%d d, %d h, %d m, and %d s", days, hours, mins, secs);
  }

  public static String convertMsToSimpleDate(long Millis) {
    DateFormat formatter = new SimpleDateFormat("MM-dd-yyyy");
    return formatter.format(new Date(Millis));
  }

  public static ByteBuffer generateNewByteBufferFromThriftRPCResults(ByteBuffer data) {
    // TODO this is a trick to fix the issue in thrift. Change the code to use
    // metadata directly when thrift fixes the issue.
    ByteBuffer correctData = ByteBuffer.allocate(data.limit() - data.position());
    correctData.put(data);
    correctData.flip();
    return correctData;
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

  public static long getCurrentMs() {
    return System.currentTimeMillis();
  }

  public static long getCurrentNs() {
    return System.nanoTime();
  }

  public static long getMB(long bytes) {
    return bytes / Constants.MB;
  }

  /**
   * Check if the given path is the root.
   * 
   * @param path
   *          The path to check
   * @return true if the path is the root
   */
  public static boolean isRoot(String path) {
    return (path.length() == 1 && path.equals(Constants.PATH_SEPARATOR));
  }

  /**
   * Get the name of the file at a path.
   * 
   * @param path
   *          The path
   * @return the name of the file
   */
  public static String getName(String path) {
    return path.substring(path.lastIndexOf('/') + 1);
  }

  /**
   * Add the path component to the base path
   * 
   * @param basePath
   *          The path to add on to
   * @param pathComponent
   *          The component to add
   * @return the concatenated path
   */
  public static String concat(Object basePathObj, Object pathComponent) {
    String basePath = basePathObj.toString();
    if (basePath.charAt(basePath.length()-1) == '/') {
      return basePath + pathComponent;
    }
    return basePath + Constants.PATH_SEPARATOR + pathComponent;
  }

  /**
   * Get the path components of the given path.
   * 
   * @param path
   *          The path to split
   * @return the path split into components
   */
  public static String[] getPathComponents(String path) throws InvalidPathException {
    validatePath(path);
    if (isRoot(path)) {
      String[] ret = new String[1];
      ret[0] = "";
      return ret;
    }
    return path.split(Constants.PATH_SEPARATOR);
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

  public static void illegalArgumentException(Exception e) {
    LOG.error(e.getMessage(), e);
    throw new IllegalArgumentException(e);
  }

  public static void illegalArgumentException(String msg) {
    throw new IllegalArgumentException(msg);
  }

  public static <T> String listToString(List<T> list) {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < list.size(); k ++) {
      sb.append(list.get(k)).append(" ");
    }
    return sb.toString();
  }

  public static boolean mkdirs(String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);
    return ufs.mkdirs(path, true);
  }

  public static String parametersToString(Object... objs) {
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

  /**
   * Parse InetSocketAddress from a String
   * 
   * @param address
   * @return
   * @throws IOException
   */
  public static InetSocketAddress parseInetSocketAddress(String address) throws IOException {
    if (address == null) {
      return null;
    }
    String[] strArr = address.split(":");
    if (strArr.length != 2) {
      throw new IOException("Invalid InetSocketAddress " + address);
    }
    return new InetSocketAddress(strArr[0], Integer.parseInt(strArr[1]));
  }

  public static long parseMemorySize(String memorySize) {
    double alpha = 0.0001;
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
    double ret = Double.parseDouble(memorySize);
    end = end.toLowerCase();
    if (end.isEmpty() || end.equals("b")) {
      return (long) (ret + alpha);
    } else if (end.equals("kb")) {
      return (long) (ret * Constants.KB + alpha);
    } else if (end.equals("mb")) {
      return (long) (ret * Constants.MB + alpha);
    } else if (end.equals("gb")) {
      return (long) (ret * Constants.GB + alpha);
    } else if (end.equals("tb")) {
      return (long) (ret * Constants.TB + alpha);
    } else if (end.equals("pb")) {
      // When parsing petabyte values, we can't multiply with doubles and longs, since that will
      // lose presicion with such high numbers. Therefore we use a BigDecimal.
      BigDecimal PBDecimal = new BigDecimal(Constants.PB);
      return PBDecimal.multiply(BigDecimal.valueOf(ret)).longValue();
    } else {
      runtimeException("Fail to parse " + ori + " as memory size");
      return -1;
    }
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

  static void redirectStreamAsync(final InputStream input, final PrintStream output) {
    new Thread(new Runnable() {
      @Override
      public void run() {
        Scanner scanner = new Scanner(input);
        while (scanner.hasNextLine()) {
          output.println(scanner.nextLine());
        }
        scanner.close();
      }
    }).start();
  }

  public static void runtimeException(Exception e) {
    LOG.error(e.getMessage(), e);
    throw new RuntimeException(e);
  }

  public static void runtimeException(String msg) {
    throw new RuntimeException(msg);
  }

  /**
   * If the sticky bit of the 'file' is set, the 'file' is only writable to its owner and the owner
   * of the folder containing the 'file'.
   * 
   * @param file
   *          absolute file path
   */
  public static void setLocalFileStickyBit(String file) {
    try {
      // sticky bit is not implemented in PosixFilePermission
      if (file.startsWith(Constants.PATH_SEPARATOR)) {
        Runtime.getRuntime().exec("chmod o+t " + file);
      }
    } catch (IOException e) {
      LOG.info("Can not set the sticky bit of the file : " + file);
    }
  }

  public static void sleepMs(Logger logger, long timeMs) {
    try {
      Thread.sleep(timeMs);
    } catch (InterruptedException e) {
      logger.warn(e.getMessage(), e);
    }
  }

  public static void tempoaryLog(String msg) {
    LOG.info("Temporary Log ============================== " + msg);
  }

  public static String[] toStringArray(ArrayList<String> src) {
    String[] ret = new String[src.size()];
    return src.toArray(ret);
  }

  /**
   * Create an empty file
   * 
   * @throws IOException
   */
  public static void touch(String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);
    OutputStream os = ufs.create(path);
    os.close();
  }

  public static void validatePath(String path) throws InvalidPathException {
    if (path == null || !path.startsWith(Constants.PATH_SEPARATOR)
        || (path.length() > 1 && path.endsWith(Constants.PATH_SEPARATOR)) || path.contains(" ")) {
      throw new InvalidPathException("Path " + path + " is invalid.");
    }
  }

  private CommonUtils() {
  }
}
