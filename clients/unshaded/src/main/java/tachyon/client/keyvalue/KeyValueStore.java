package tachyon.client.keyvalue;

import java.io.IOException;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.exception.TachyonException;

/**
 * KeyValue client to access key-value data stored in Tachyon.
 */
@PublicApi
public interface KeyValueStore {
  /**
   * Gets a reader to access a key-value store.
   *
   * @param uri {@link TachyonURI} to the store
   * @return {@link BaseKeyValueStoreReader} instance
   * @throws IOException if non-Tachyon error occurs
   * @throws TachyonException if Tachyon error occurs
   */
  KeyValueStoreReader open(TachyonURI uri) throws IOException, TachyonException;

  /**
   * Gets a writer to create a new key-value store.
   *
   * @param uri {@link TachyonURI} to the store
   * @return {@link BaseKeyValueStoreWriter} instance
   * @throws IOException if non-Tachyon error occurs
   * @throws TachyonException if Tachyon error occurs
   */
  KeyValueStoreWriter create(TachyonURI uri) throws IOException, TachyonException;
}
