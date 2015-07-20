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

package tachyon;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

public final class TestUtils {

  public static byte[] getIncreasingByteArray(int len) {
    return getIncreasingByteArray(0, len);
  }

  public static byte[] getIncreasingByteArray(int start, int len) {
    byte[] ret = new byte[len];
    for (int k = 0; k < len; k ++) {
      ret[k] = (byte) (k + start);
    }
    return ret;
  }

  public static boolean equalIncreasingByteArray(int len, byte[] arr) {
    return equalIncreasingByteArray(0, len, arr);
  }

  public static boolean equalIncreasingByteArray(int start, int len, byte[] arr) {
    if (arr == null || arr.length < len) {
      return false;
    }
    for (int k = 0; k < len; k ++) {
      if (arr[k] != (byte) (start + k)) {
        return false;
      }
    }
    return true;
  }

  public static ByteBuffer getIncreasingByteBuffer(int len) {
    return getIncreasingByteBuffer(0, len);
  }

  public static ByteBuffer getIncreasingByteBuffer(int start, int len) {
    return ByteBuffer.wrap(getIncreasingByteArray(start, len));
  }

  public static boolean equalIncreasingByteBuffer(int start, int len, ByteBuffer buf) {
    if (buf == null) {
      return false;
    }
    buf.rewind();
    if (buf.remaining() != len) {
      return false;
    }
    for (int k = 0; k < len; k ++) {
      if (buf.get() != (byte) (start + k)) {
        return false;
      }
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

  public static int getToMasterHeartBeatIntervalMs(TachyonConf tachyonConf) {
    return tachyonConf
        .getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS, Constants.SECOND_MS);
  }

  /**
   * Creates a unique path based off the caller.
   */
  public static final String uniqPath() {
    StackTraceElement caller = new Throwable().getStackTrace()[1];
    long time = System.nanoTime();
    return "/" + caller.getClassName() + "/" + caller.getMethodName() + "/" + time;
  }

  /**
   * * Writes buffer to the given file path
   *
   * @param path file path to write the data
   * @param buffer raw data
   * @throws IOException
   */
  public static void writeBufferToFile(String path, byte[] buffer) throws IOException {
    FileOutputStream os = new FileOutputStream(path);
    os.write(buffer);
    os.close();
  }
}
