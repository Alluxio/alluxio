/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.thrift.InvalidPathException;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

/**
 * Common utilities shared by all components in Tachyon.
 */
public final class CommonUtils {
  private static final Logger LOG = LoggerFactory.getLogger("");

  /**
   * Change local file's permission.
   *
   * @param filePath that will change permission
   * @param perms the permission, e.g. "775"
   * @throws IOException
   */
  public static void changeLocalFilePermission(String filePath, String perms) throws IOException {
    // TODO switch to java's Files.setPosixFilePermissions() if java 6 support is dropped
    List<String> commands = new ArrayList<String>();
    commands.add("/bin/chmod");
    commands.add(perms);
    File file = new File(filePath);
    commands.add(file.getAbsolutePath());

    try {
      ProcessBuilder builder = new ProcessBuilder(commands);
      Process process = builder.start();

      process.waitFor();

      redirectIO(process);

      if (process.exitValue() != 0) {
        throw new IOException("Can not change the file " + file.getAbsolutePath()
            + " 's permission to be " + perms);
      }
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
      throw new IOException(e);
    }
  }

  /**
   * Creates the local block path and all the parent directories. Also, sets the appropriate
   * permissions.
   *
   * @param path The path of the block.
   * @throws IOException
   */
  public static void createBlockPath(String path) throws IOException {
    File localFolder;
    try {
      localFolder = new File(CommonUtils.getParent(path));
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }

    if (!localFolder.exists()) {
      if (localFolder.mkdirs()) {
        CommonUtils.changeLocalFileToFullPermission(localFolder.getAbsolutePath());
        LOG.info("Folder {} was created!", localFolder);
      } else {
        throw new IOException("Failed to create folder " + localFolder);
      }
    }
  }

  /**
   * Blocking operation that copies the processes stdout/stderr to this JVM's stdout/stderr.
   */
  private static void redirectIO(final Process process) throws IOException {
    // Because chmod doesn't have a lot of error or output messages, its safe to process the output
    // after the process is done. As of java 7, you can have the process redirect to System.out
    // and System.err without forking a process.
    // TODO when java 6 support is dropped, switch to
    // http://docs.oracle.com/javase/7/docs/api/java/lang/ProcessBuilder.html#inheritIO()
    Closer closer = Closer.create();
    try {
      ByteStreams.copy(closer.register(process.getInputStream()), System.out);
      ByteStreams.copy(closer.register(process.getErrorStream()), System.err);
    } catch (Throwable e) {
      throw closer.rethrow(e);
    } finally {
      closer.close();
    }
  }

  /**
   * Change local file's permission to be 777.
   *
   * @param filePath that will change permission
   * @throws IOException
   */
  public static void changeLocalFileToFullPermission(String filePath) throws IOException {
    changeLocalFilePermission(filePath, "777");
  }

  /**
   * Force to unmap direct buffer if the buffer is no longer used. It is unsafe operation and
   * currently a walk-around to avoid huge memory occupation caused by memory map.
   *
   * @param buffer the byte buffer to be unmapped
   */
  public static void cleanDirectBuffer(ByteBuffer buffer) {
    if (buffer == null) {
      return;
    }
    if (buffer.isDirect()) {
      Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
      cleaner.clean();
    }
  }

  /**
   * Checks and normalizes the given path
   *
   * @param path The path to clean up
   * @return a normalized version of the path, with single separators between path components and
   *         dot components resolved
   */
  public static String cleanPath(String path) throws InvalidPathException {
    validatePath(path);
    return FilenameUtils.separatorsToUnix(FilenameUtils.normalizeNoEndSeparator(path));
  }

  public static ByteBuffer cloneByteBuffer(ByteBuffer buf) {
    ByteBuffer ret = ByteBuffer.allocate(buf.limit() - buf.position());
    ret.put(buf.array(), buf.position(), buf.limit() - buf.position());
    ret.flip();
    return ret;
  }

  public static List<ByteBuffer> cloneByteBufferList(List<ByteBuffer> source) {
    List<ByteBuffer> ret = new ArrayList<ByteBuffer>(source.size());
    for (ByteBuffer b : source) {
      ret.add(cloneByteBuffer(b));
    }
    return ret;
  }

  /**
   * Join each element in paths in order, separated by {@code TachyonURI.SEPARATOR}.
   * <p>
   * For example, {@code concatPath("/myroot/", "dir", 1L, "filename")} returns
   * {@code "/myroot/dir/1/filename"}, or
   * {@code concatPath("tachyon://myroot", "dir", "filename")} returns
   * {@code "tachyon://myroot/dir/filename"}. Note that a null element in paths
   * is treated as empty string, i.e., "".
   *
   * @param paths to concatenate
   * @return joined path
   * @throws IllegalArgumentException
   */
  public static String concatPath(Object... paths) throws IllegalArgumentException {
    if (null == paths) {
      throw new IllegalArgumentException("Can not concatenate a null set of paths.");
    }
    String path;
    String trimmedPath;
    List<String> trimmedPathList = new ArrayList<String>();
    if (paths.length > 0 && paths[0] != null && !paths[0].toString().isEmpty()) {
      path = paths[0].toString().trim();
      trimmedPath = CharMatcher.is(TachyonURI.SEPARATOR.charAt(0)).trimTrailingFrom(path);
      trimmedPathList.add(trimmedPath);
    }
    for (int k = 1; k < paths.length; k ++) {
      if (null == paths[k]) {
        continue;
      }
      path = paths[k].toString().trim();
      trimmedPath = CharMatcher.is(TachyonURI.SEPARATOR.charAt(0)).trimFrom(path);
      if (!trimmedPath.isEmpty()) {
        trimmedPathList.add(trimmedPath);
      }
    }
    if (trimmedPathList.size() == 1 && trimmedPathList.get(0).isEmpty()) {
      // paths[0] must be "[/]+"
      return TachyonURI.SEPARATOR;
    }
    return Joiner.on(TachyonURI.SEPARATOR).join(trimmedPathList);
  }

  public static ByteBuffer generateNewByteBufferFromThriftRPCResults(ByteBuffer data) {
    // TODO this is a trick to fix the issue in thrift. Change the code to use
    // metadata directly when thrift fixes the issue.
    ByteBuffer correctData = ByteBuffer.allocate(data.limit() - data.position());
    correctData.put(data);
    correctData.flip();
    return correctData;
  }

  public static long getCurrentMs() {
    return System.currentTimeMillis();
  }

  /**
   * Get the parent of the file at a path.
   *
   * @param path The path
   * @return the parent path of the file; this is "/" if the given path is the root.
   * @throws InvalidPathException
   */
  public static String getParent(String path) throws InvalidPathException {
    String cleanedPath = cleanPath(path);
    String name = FilenameUtils.getName(cleanedPath);
    String parent = cleanedPath.substring(0, cleanedPath.length() - name.length() - 1);
    if (parent.isEmpty()) {
      // The parent is the root path
      return TachyonURI.SEPARATOR;
    }
    return parent;
  }

  /**
   * Get the path components of the given path.
   *
   * @param path The path to split
   * @return the path split into components
   * @throws InvalidPathException
   */
  public static String[] getPathComponents(String path) throws InvalidPathException {
    path = cleanPath(path);
    if (isRoot(path)) {
      String[] ret = new String[1];
      ret[0] = "";
      return ret;
    }
    return path.split(TachyonURI.SEPARATOR);
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
    if (ret <= 1024 * 5) {
      return String.format("%.2f GB", ret);
    }
    ret /= 1024;
    if (ret <= 1024 * 5) {
      return String.format("%.2f TB", ret);
    }
    return String.format("%.2f PB", ret);
  }

  /**
   * Check if the given path is the root.
   *
   * @param path The path to check
   * @return true if the path is the root
   * @throws InvalidPathException
   */
  public static boolean isRoot(String path) throws InvalidPathException {
    return TachyonURI.SEPARATOR.equals(cleanPath(path));
  }

  public static <T> String listToString(List<T> list) {
    StringBuilder sb = new StringBuilder();
    for (T s : list) {
      sb.append(s).append(" ");
    }
    return sb.toString();
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
   * @return InetSocketAddress of the String
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

  /**
   * Parse a String size to Bytes.
   *
   * @param spaceSize the size of a space, e.g. 10GB, 5TB, 1024
   * @return the space size in bytes
   */
  public static long parseSpaceSize(String spaceSize) {
    double alpha = 0.0001;
    String ori = spaceSize;
    String end = "";
    int tIndex = spaceSize.length() - 1;
    while (tIndex >= 0) {
      if (spaceSize.charAt(tIndex) > '9' || spaceSize.charAt(tIndex) < '0') {
        end = spaceSize.charAt(tIndex) + end;
      } else {
        break;
      }
      tIndex --;
    }
    spaceSize = spaceSize.substring(0, tIndex + 1);
    double ret = Double.parseDouble(spaceSize);
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
      BigDecimal pBDecimal = new BigDecimal(Constants.PB);
      return pBDecimal.multiply(BigDecimal.valueOf(ret)).longValue();
    } else {
      throw new IllegalArgumentException("Fail to parse " + ori + " to bytes");
    }
  }

  public static void printByteBuffer(Logger LOG, ByteBuffer buf) {
    StringBuilder sb = new StringBuilder();
    for (int k = 0; k < buf.limit() / 4; k ++) {
      sb.append(buf.getInt()).append(" ");
    }

    LOG.info(sb.toString());
  }

  public static void printTimeTakenMs(long startTimeMs, Logger logger, String message) {
    logger.info(message + " took " + (getCurrentMs() - startTimeMs) + " ms.");
  }

  public static void printTimeTakenNs(long startTimeNs, Logger logger, String message) {
    logger.info(message + " took " + (System.nanoTime() - startTimeNs) + " ns.");
  }

  public static void putIntByteBuffer(ByteBuffer buf, int b) {
    buf.put((byte) (b & 0xFF));
  }

  /**
   * If the sticky bit of the 'file' is set, the 'file' is only writable to its owner and the owner
   * of the folder containing the 'file'.
   *
   * @param file absolute file path
   */
  public static void setLocalFileStickyBit(String file) {
    try {
      // sticky bit is not implemented in PosixFilePermission
      if (file.startsWith(TachyonURI.SEPARATOR)) {
        Runtime.getRuntime().exec("chmod o+t " + file);
      }
    } catch (IOException e) {
      LOG.info("Can not set the sticky bit of the file : " + file);
    }
  }

  public static void sleepMs(Logger logger, long timeMs) {
    sleepMs(logger, timeMs, false);
  }

  public static void sleepMs(Logger logger, long timeMs, boolean shouldInterrupt) {
    try {
      Thread.sleep(timeMs);
    } catch (InterruptedException e) {
      logger.warn(e.getMessage(), e);
      if (shouldInterrupt) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static String[] toStringArray(ArrayList<String> src) {
    String[] ret = new String[src.size()];
    return src.toArray(ret);
  }

  /**
   * Check if the given path is properly formed
   *
   * @param path The path to check
   * @throws InvalidPathException If the path is not properly formed
   */
  public static void validatePath(String path) throws InvalidPathException {
    if (path == null || path.isEmpty() || !path.startsWith(TachyonURI.SEPARATOR)
        || path.contains(" ")) {
      throw new InvalidPathException("Path " + path + " is invalid.");
    }
  }

  /**
   * Creates new instance of a class by calling a constructor that receives ctorClassArgs arguments
   *
   * @param cls the class to create
   * @param ctorClassArgs parameters type list of the constructor to initiate, if null default
   *        constructor will be called
   * @param ctorArgs the arguments to pass the constructor
   * @return new class object or null if not successful
   */
  public static <T> T createNewClassInstance(Class<T> cls, Class<?>[] ctorClassArgs,
      Object[] ctorArgs) throws InstantiationException, IllegalAccessException,
      NoSuchMethodException, SecurityException, InvocationTargetException {
    if (ctorClassArgs == null) {
      return cls.newInstance();
    }
    Constructor<T> ctor = cls.getConstructor(ctorClassArgs);
    return ctor.newInstance(ctorArgs);
  }
}
