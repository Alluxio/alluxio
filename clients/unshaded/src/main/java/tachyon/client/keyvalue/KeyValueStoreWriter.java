package tachyon.client.keyvalue;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.annotation.PublicApi;
import tachyon.exception.TachyonException;

/**
 * Interface for readers to create a new key-value store in Tachyon.
 */
@PublicApi
public interface KeyValueStoreWriter extends Closeable {
  /**
   * Adds a key and its associated value to this store.
   *
   * @param key key to put, cannot be null
   * @param value value to put, cannot be null
   * @throws IOException if non-Tachyon error occurs
   * @throws TachyonException if Tachyon error occurs
   */
  void put(byte[] key, byte[] value) throws IOException, TachyonException;

  /**
   * Adds a key and its associated value to this store.
   *
   * @param key key to put, cannot be null
   * @param value value to put, cannot be null
   * @throws IOException if non-Tachyon error occurs
   * @throws TachyonException if Tachyon error occurs
   */
  void put(ByteBuffer key, ByteBuffer value) throws IOException, TachyonException;

}
