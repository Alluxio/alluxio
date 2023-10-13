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

import alluxio.resource.CloseableIterator;

import com.google.common.primitives.Longs;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Convenience methods for working with RocksDB.
 */
public final class RocksUtils {
  private static final Logger LOG = LoggerFactory.getLogger(RocksUtils.class);

  private RocksUtils() {} // Utils class.

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
  public interface RocksIteratorParser<T> {
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
   * Used to wrap an {@link CloseableIterator} over {@link RocksIterator}.
   * It seeks given iterator to first entry before returning the iterator.
   *
   * The Iterator is associated with a shared lock to the RocksStore. The lock should be acquired
   * by the caller (See java doc on RocksStore.checkAndAcquireSharedLock()) for how.
   * And the lock is held throughout the lifecycle of this iterator until it is closed
   * either on completion or on exception. This shared lock guarantees thread safety when
   * accessing the RocksDB. In other words, when this shared lock is held, the underlying
   * RocksDB will not be stopped/restarted.
   *
   * The abortCheck defines a way to voluntarily abort the iteration. This typically happens
   * when the underlying RocksDB will be closed/restart/checkpointed, where all accesses should
   * be stopped.
   *
   * With the thread safety baked into hasNext() and next(), users of this Iterator do not need
   * to worry about safety and can use this Iterator normally.
   * See examples in how this iterator is used in RocksBlockMetaStore and RocksInodeStore.
   *
   * @param rocksIterator the rocks iterator
   * @param parser parser to produce iterated values from rocks key-value
   * @param <T> iterator value type
   * @param abortCheck if true, abort the iteration
   * @param rocksDbSharedLock the shared lock acquired by the iterator
   * @return wrapped iterator
   */
  public static <T> CloseableIterator<T> createCloseableIterator(
        RocksIterator rocksIterator, RocksIteratorParser<T> parser,
        Supplier<Void> abortCheck, RocksSharedLockHandle rocksDbSharedLock) {
    rocksIterator.seekToFirst();
    AtomicBoolean valid = new AtomicBoolean(true);
    Iterator<T> iter = new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return valid.get() && rocksIterator.isValid();
      }

      @Override
      public T next() {
        boolean succeeded = false;

        /*
         * If the RocksDB wants to stop, abort the loop instead of finishing it.
         * The abortCheck will throw an exception, which closes the CloseableIterator
         * if the CloseableIterator is correctly put in a try-with-resource section.
         */
        abortCheck.get();

        try {
          T result = parser.next(rocksIterator);
          rocksIterator.next();
          succeeded = true;
          return result;
        } catch (Exception exc) {
          LOG.warn("Iteration aborted because of error", exc);
          throw new RuntimeException(exc);
        } finally {
          if (!succeeded) {
            valid.set(false);
            rocksIterator.close();
          }
        }
      }
    };

    return CloseableIterator.create(iter, (whatever) -> {
      try {
        rocksIterator.close();
      } finally {
        if (rocksDbSharedLock != null) {
          // Release the lock after recycling the iterator safely
          rocksDbSharedLock.close();
        }
      }
    });
  }
}
