package tachyon.client.keyvalue;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.annotation.PublicApi;
import tachyon.exception.TachyonException;

/**
 * Interface for readers to access a key-value store in Tachyon.
 */
@PublicApi
public interface KeyValueStoreReader extends Closeable {
  /**
   * Gets the value associated with {@code key}, returns null if not found.
   *
   * @param key key to put, cannot be null
   * @return value associated with the given key, or null if not found
   * @throws IOException if non-Tachyon error occurs
   * @throws TachyonException if Tachyon error occurs
   */
  ByteBuffer get(ByteBuffer key) throws IOException, TachyonException;
}
