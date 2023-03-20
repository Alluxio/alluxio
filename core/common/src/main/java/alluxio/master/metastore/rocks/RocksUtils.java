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

import alluxio.resource.LockResource;
import com.google.common.primitives.Longs;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

// TODO(jiacheng): move to alluxio.rocks
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
   * The abort check is checked in hasNext(), where we check whether the RocksDB is closed and
   * iteration should end. However, hasNext() and next() is a check-then-act race condition,
   * where RocksDB may be closed immediately after hasNext() and before next(). We avoid that by
   * acquiring a lock in next() access. The lock should guarantee safety during the data access.
   *
   * With the thread safety baked into hasNext() and next(), users of this Iterator do not need
   * to worry about safety and can use this Iterator normally.
   * See examples in how this iterator is used in RocksBlockMetaStore and RocksInodeStore.
   *
   * @param rocksIterator the rocks iterator
   * @param parser parser to produce iterated values from rocks key-value
   * @param <T> iterator value type
   * @param abortCheck if true, abort the iteration
   * @param locker acquire a lock in next() to ensure thread safety, if any
   * @return wrapped iterator
   */
  public static <T> CloseableIterator<T> createCloseableIterator(
      RocksIterator rocksIterator, RocksIteratorParser<T> parser,
      Supplier<Boolean> abortCheck, Supplier<LockResource> locker) {
    rocksIterator.seekToFirst();
    AtomicBoolean valid = new AtomicBoolean(true);
    Iterator<T> iter = new Iterator<T>() {
      @Override
      public boolean hasNext() {
        /*
         * There can be a race condition where after passing the hasNext() check, the RocksDB
         * is closed and next() causes segfault. Avoiding that requires the closeCheck to stop the
         * iteration way before entering the critical section. That means we can use the
         * closeCheck to stop iteration when the iterator is still valid, then safely close
         * RocksDB and all relevant references without worrying about concurrent readers
         * like this escaped iterator.
         */
        return (!abortCheck.get()) && valid.get() && rocksIterator.isValid();
      }

      @Override
      public T next() {
        boolean succeeded = false;
        // This lock will assure the RocksDB is not closed while this is reading
        try (LockResource lock = locker.get()){
          T result = parser.next(rocksIterator);
          rocksIterator.next();
          succeeded = true;
          return result;
        } catch (Exception exc) {
          LOG.warn("Iteration aborted because of error", exc);
          throw new RuntimeException(exc);
        } catch (Throwable t) {
          LOG.warn("Iteration aborted because of error", t);
          throw t;
        } finally {
          if (!succeeded) {
            valid.set(false);
            rocksIterator.close();
          }
        }
      }
    };

    return CloseableIterator.create(iter, (whatever) -> rocksIterator.close());
  }
}
