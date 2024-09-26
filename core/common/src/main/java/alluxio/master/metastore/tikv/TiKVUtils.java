package alluxio.master.metastore.tikv;

import alluxio.resource.CloseableIterator;
import com.google.common.primitives.Longs;
import org.tikv.kvproto.Kvrpcpb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Convenience methods for working with TiKV.
 */
public final class TiKVUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TiKVUtils.class);

    private TiKVUtils() {} // Utils class.


    /**
     * @param str a String value
     * @param long1 a long value
     * @param long2 a long value
     * @return a byte array formed by writing the bytes of n followed by the bytes of str
     */
    public static byte[] toByteArray(String str, long long1, long long2) {
        byte[] strBytes = str.getBytes();

        byte[] key = new byte[strBytes.length + 2 * Longs.BYTES];
        System.arraycopy(strBytes, 0, key, 0, strBytes.length);
        for (int i = strBytes.length + Longs.BYTES - 1; i >= strBytes.length; i--) {
            key[i] = (byte) (long1 & 0xffL);
            long1 >>= Byte.SIZE;
        }
        for (int i = strBytes.length + 2 * Longs.BYTES - 1; i >= strBytes.length + Longs.BYTES; i--) {
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
    public static byte[] toByteArray(String str, long n) {
        byte[] strBytes = str.getBytes();

        byte[] key = new byte[Longs.BYTES + strBytes.length];
        System.arraycopy(strBytes, 0, key, 0, strBytes.length);
        for (int i = key.length - 1; i >= strBytes.length; i--) {
            key[i] = (byte) (n & 0xffL);
            n >>= Byte.SIZE;
        }
        return key;
    }

    /**
     * @param n a long value
     * @param str1 a string value
     * @param str2 a string value
     * @return a byte array formed by writing the bytes of n followed by the bytes of str
     */
    public static byte[] toByteArray(String str1, long n, String str2) {
        byte[] strBytes1 = str1.getBytes();
        byte[] strBytes2 = str2.getBytes();

        byte[] key = new byte[Longs.BYTES + strBytes1.length + strBytes2.length];
        System.arraycopy(strBytes1, 0, key, 0, strBytes1.length);
        for (int i = strBytes1.length + Longs.BYTES - 1; i >= strBytes1.length; i--) {
            key[i] = (byte) (n & 0xffL);
            n >>= Byte.SIZE;
        }
        System.arraycopy(strBytes2, 0, key, strBytes1.length + Longs.BYTES, strBytes2.length);
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
     * Used to parse current {@link ListIterator<Kvrpcpb.KvPair>} element.
     *
     * @param <T> return type of parser's next method
     */
    public interface TiKVIteratorParser<T> {
        /**
         * Parses and return next element.
         *
         * @param iter {@link ListIterator<Kvrpcpb.KvPair>} instance
         * @return parsed value
         * @throws Exception if parsing fails
         */
        T next(ListIterator<Kvrpcpb.KvPair> iter) throws Exception;
    }

    /**
     * Used to wrap an {@link CloseableIterator} over {@link ListIterator<Kvrpcpb.KvPair>}.
     * It seeks given iterator to first entry before returning the iterator.
     *
     * @param tikvIterator the tikv iterator
     * @param parser parser to produce iterated values from tikv key-value
     * @param <T> iterator value type
     * @return wrapped iterator
     */
    public static <T> CloseableIterator<T> createCloseableIterator(
            ListIterator<Kvrpcpb.KvPair> tikvIterator, TiKVIteratorParser<T> parser) {
        AtomicBoolean valid = new AtomicBoolean(true);
        Iterator<T> iter = new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return valid.get() && tikvIterator.hasNext();
            }

            @Override
            public T next() {
                try {
                    return parser.next(tikvIterator);
                } catch (Exception exc) {
                    LOG.warn("Iteration aborted because of error", exc);
                    valid.set(false);
                    throw new RuntimeException(exc);
                } finally {
                    if (!tikvIterator.hasNext()) {
                        valid.set(false);
                    }
                }
            }
        };

        return CloseableIterator.noopCloseable(iter);
    }

}
