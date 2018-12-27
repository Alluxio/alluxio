/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.metastore.rocks;

import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

/**
 * Convenience methods for working with RocksDB.
 */
public final class RocksUtils {

  private RocksUtils() {} // Utils class.

  /**
   * Generates a path to use for a RocksDB database. This method incorporates randomness to avoid
   * creating multiple databases at the same path.
   *
   * @param baseDir the base directory path
   * @param dbName a name for the database
   * @return the generated database path
   */
  public static String generateDbPath(String baseDir, String dbName) {
    return PathUtils.concatPath(baseDir, String.format("%s-%s-%s", dbName,
        System.currentTimeMillis(), CommonUtils.randomAlphaNumString(3)));
  }

  /**
   * @param long1 a long value
   * @param long2 a long value
   * @return a byte array formed by writing the two long values as bytes
   */
  public static byte[] toByteArray(long long1, long long2) {
    byte[] key = new byte[16];
    for (int i = 7; i >= 0; i--) {
      key[i] = (byte) (long1 & 0xffL);
      long1 >>= 8;
    }
    for (int i = 15; i >= 8; i--) {
      key[i] = (byte) (long2 & 0xffL);
      long2 >>= 8;
    }
    return key;
  }

  /**
   * @param n a long value
   * @param str a string value
   * @return a byte array formed by writing the bytes of n followed by the bytes of str
   */
  public static byte[] toByteArray(long n, String str) {
    byte[] strBytes = str.getBytes();

    byte[] key = new byte[8 + strBytes.length];
    for (int i = 7; i >= 0; i--) {
      key[i] = (byte) (n & 0xffL);
      n >>= 8;
    }
    System.arraycopy(strBytes, 0, key, 8, strBytes.length);
    return key;
  }
}
