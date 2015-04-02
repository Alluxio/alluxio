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

package tachyon.perf.benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import tachyon.perf.fs.PerfFileSystem;

/**
 * This class contains a set of file operators which can be used in different benchmarks.
 */
public class Operators {
  private static final Random RAND = new Random(System.currentTimeMillis());

  /**
   * Skip forward and read the file for times.
   * 
   * @param fs
   * @param filePath
   * @param bufferSize
   * @param skipBytes
   * @param readBytes
   * @param readType
   * @param times
   * @return the actual number of bytes read
   * @throws IOException
   */
  public static long forwardSkipRead(PerfFileSystem fs, String filePath, int bufferSize,
      long skipBytes, long readBytes, String readType, int times) throws IOException {
    byte[] content = new byte[bufferSize];
    long readLen = 0;
    InputStream is = fs.open(filePath, readType);
    for (int t = 0; t < times; t ++) {
      is.skip(skipBytes);
      readLen += readSpecifiedBytes(is, content, readBytes);
    }
    is.close();
    return readLen;
  }

  /**
   * Do metadata operations. These metadata operations consist of mkdir, touch, exists, rename and
   * delete.
   * 
   * @param fs
   * @param filePath
   * @return the actual number of finished metadata operations
   * @throws IOException
   */
  public static int metadataSample(PerfFileSystem fs, String filePath) throws IOException {
    String emptyFilePath = filePath + "/metadata-test-file";
    if (!fs.mkdirs(filePath, true)) {
      return 0;
    }
    if (!fs.createEmptyFile(emptyFilePath)) {
      return 1;
    }
    if (!fs.exists(emptyFilePath)) {
      return 2;
    }
    if (!fs.rename(emptyFilePath, emptyFilePath + "-__-__-")) {
      return 3;
    }
    if (!fs.delete(filePath, true)) {
      return 4;
    }
    return 5;
  }

  /**
   * Skip randomly then read the file for times.
   * 
   * @param fs
   * @param filePath
   * @param bufferSize
   * @param readBytes
   * @param readType
   * @param times
   * @return the actual number of bytes read
   * @throws IOException
   */
  public static long randomSkipRead(PerfFileSystem fs, String filePath, int bufferSize,
      long readBytes, String readType, int times) throws IOException {
    byte[] content = new byte[bufferSize];
    long readLen = 0;
    long fileLen = fs.getLength(filePath);
    for (int t = 0; t < times; t ++) {
      long skipBytes = RAND.nextLong() % fileLen;
      if (skipBytes < 0) {
        skipBytes = -skipBytes;
      }
      InputStream is = fs.open(filePath, readType);
      is.skip(skipBytes);
      readLen += readSpecifiedBytes(is, content, readBytes);
      is.close();
    }
    return readLen;
  }

  /**
   * Read a file from begin to the end.
   * 
   * @param fs
   * @param filePath
   * @param bufferSize
   * @return the actual number of bytes read
   * @throws IOException
   */
  public static long readSingleFile(PerfFileSystem fs, String filePath, int bufferSize)
      throws IOException {
    return readSingleFile(fs, filePath, bufferSize, "NO_CACHE");
  }

  /**
   * Read a file from begin to the end.
   * 
   * @param fs
   * @param filePath
   * @param bufferSize
   * @param readType
   * @return the actual number of bytes read
   * @throws IOException
   */
  public static long readSingleFile(PerfFileSystem fs, String filePath, int bufferSize,
      String readType) throws IOException {
    long readLen = 0;
    byte[] content = new byte[bufferSize];
    InputStream is = fs.open(filePath, readType);
    int onceLen = is.read(content);
    while (onceLen > 0) {
      readLen += (long) onceLen;
      onceLen = is.read(content);
    }
    is.close();
    return readLen;
  }

  private static long readSpecifiedBytes(InputStream is, byte[] content, long readBytes)
      throws IOException {
    long remainBytes = readBytes;
    int readLen = 0;
    while (remainBytes >= content.length) {
      int once = is.read(content);
      if (once <= 0) {
        return readLen;
      }
      readLen += once;
      remainBytes -= once;
    }
    if (remainBytes > 0) {
      int once = is.read(content, 0, (int) remainBytes);
      if (once <= 0) {
        return readLen;
      }
      readLen += once;
    }
    return readLen;
  }

  private static void writeContentToFile(OutputStream os, long fileSize, int bufferSize)
      throws IOException {
    byte[] content = new byte[bufferSize];
    long remain = fileSize;
    while (remain >= bufferSize) {
      os.write(content);
      remain -= bufferSize;
    }
    if (remain > 0) {
      os.write(content, 0, (int) remain);
    }
  }

  /**
   * Create a file and write to it.
   * 
   * @param fs
   * @param filePath
   * @param fileSize
   * @param bufferSize
   * @throws IOException
   */
  public static void writeSingleFile(PerfFileSystem fs, String filePath, long fileSize,
      int bufferSize) throws IOException {
    OutputStream os = fs.create(filePath);
    writeContentToFile(os, fileSize, bufferSize);
    os.close();
  }

  /**
   * Create a file and write to it.
   * 
   * @param fs
   * @param filePath
   * @param fileSize
   * @param blockSize
   * @param bufferSize
   * @throws IOException
   */
  public static void writeSingleFile(PerfFileSystem fs, String filePath, long fileSize,
      int blockSize, int bufferSize) throws IOException {
    OutputStream os = fs.create(filePath, blockSize);
    writeContentToFile(os, fileSize, bufferSize);
    os.close();
  }

  /**
   * Create a file and write to it.
   * 
   * @param fs
   * @param filePath
   * @param fileSize
   * @param blockSize
   * @param bufferSize
   * @param writeType
   * @throws IOException
   */
  public static void writeSingleFile(PerfFileSystem fs, String filePath, long fileSize,
      int blockSize, int bufferSize, String writeType) throws IOException {
    OutputStream os = fs.create(filePath, blockSize, writeType);
    writeContentToFile(os, fileSize, bufferSize);
    os.close();
  }
}
