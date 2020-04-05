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

import alluxio.util.io.PathUtils;

import com.google.common.primitives.Longs;
import org.rocksdb.RocksIterator;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Convenience methods for working with RocksDB.
 */
public final class RocksUtils {

  private RocksUtils() {} // Utils class.

  /**
   * Generates a path to use for a RocksDB database.
   *
   * @param baseDir the base directory path
   * @param dbName a name for the database
   * @return the generated database path
   */
  public static String generateDbPath(String baseDir, String dbName) {
    return PathUtils.concatPath(baseDir, dbName);
  }

  /**
   * @param long1 a long value
   * @param long2 a long value
   * @return a byte array formed by writing the two long values as bytes
   */
  public static byte[] toByteArray(long long1, long long2) {
    byte[] key = new byte[2 * Longs.BYTES];
    for (int i = Longs.BYTES - 1; i >= 0; i--) {
      key[i] = (byte) (long1 & 0xffL);
      long1 >>= Byte.SIZE;
    }
    for (int i = 2 * Longs.BYTES - 1; i >= Longs.BYTES; i--) {
      key[i] = (byte) (long2 & 0xffL);
      long2 >>= Byte.SIZE;
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

    byte[] key = new byte[Longs.BYTES + strBytes.length];
    for (int i = Longs.BYTES - 1; i >= 0; i--) {
      key[i] = (byte) (n & 0xffL);
      n >>= Byte.SIZE;
    }
    System.arraycopy(strBytes, 0, key, Longs.BYTES, strBytes.length);
    return key;
  }

  /**
   * @param bytes an array of bytes
   * @param start the place in the array to read the long from
   * @return the long
   */
  public static long readLong(byte[] bytes, int start) {
    return Longs.fromBytes(bytes[start], bytes[start + 1], bytes[start + 2], bytes[start + 3],
        bytes[start + 4], bytes[start + 5], bytes[start + 6], bytes[start + 7]);
  }

  /**
   * Used to parse current {@link RocksIterator} element.
   *
   * @param <T> return type of parser's next method
   */
  interface RocksIteratorParser<T> {
    /**
     * Parses and return next element.
     *
     * @param iter {@link RocksIterator} instance
     * @return parsed value
     * @throws Exception if parsing fails
     */
    T next(RocksIterator iter) throws Exception;
  }

  /**
   * Used to wrap an {@link Iterator} over {@link RocksIterator}.
   * It seeks given iterator to first entry before returning the iterator.
   *
   * @param rocksIterator the rocks iterator
   * @param parser parser to produce iterated values from rocks key-value
   * @param <T> iterator value type
   * @return wrapped iterator
   */
  public static <T> Iterator<T> createIterator(RocksIterator rocksIterator,
      RocksIteratorParser<T> parser) {
    rocksIterator.seekToFirst();
    AtomicBoolean valid = new AtomicBoolean(true);
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return valid.get() && rocksIterator.isValid();
      }

      @Override
      public T next() {
        try {
          return parser.next(rocksIterator);
        } catch (Exception exc) {
          rocksIterator.close();
          valid.set(false);
          throw new RuntimeException(exc);
        } finally {
          rocksIterator.next();
          if (!rocksIterator.isValid()) {
            rocksIterator.close();
            valid.set(false);
          }
        }
      }
    };
  }
}
