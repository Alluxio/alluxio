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

package alluxio.client.keyvalue;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;

import com.google.common.base.Preconditions;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Default implementation of the {@link KeyValueSystem} interface. Developers can extend this class
 * instead of implementing the interface. This implementation reads and writes key-value data
 * through {@link BaseKeyValueStoreReader} and {@link BaseKeyValueStoreWriter}.
 */
@PublicApi
@ThreadSafe
public final class BaseKeyValueSystem implements KeyValueSystem {
  private final KeyValueMasterClient mMasterClient =
      new KeyValueMasterClient(FileSystemContext.INSTANCE.getMasterAddress());

  /**
   * Constructs a new {@link BaseKeyValueSystem}.
   */
  public BaseKeyValueSystem() {}

  @Override
  public KeyValueStoreReader openStore(AlluxioURI uri) throws IOException, AlluxioException {
    Preconditions.checkNotNull(uri, PreconditionMessage.URI_KEY_VALUE_STORE_NULL);
    try {
      return new BaseKeyValueStoreReader(uri);
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    }
  }

  @Override
  public KeyValueStoreWriter createStore(AlluxioURI uri) throws IOException, AlluxioException {
    Preconditions.checkNotNull(uri, PreconditionMessage.URI_KEY_VALUE_STORE_NULL);
    try {
      return new BaseKeyValueStoreWriter(uri);
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    }
  }

  @Override
  public void deleteStore(AlluxioURI uri)
      throws IOException, InvalidPathException, FileDoesNotExistException, AlluxioException {
    try {
      mMasterClient.deleteStore(uri);
    } catch (UnavailableException e) {
      throw new IOException(e);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage(), e);
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    }
  }

  @Override
  public void renameStore(AlluxioURI oldUri, AlluxioURI newUri)
      throws IOException, AlluxioException {
    try {
      mMasterClient.renameStore(oldUri, newUri);
    } catch (UnavailableException e) {
      throw new IOException(e);
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    }
  }

  @Override
  public void mergeStore(AlluxioURI fromUri, AlluxioURI toUri)
      throws IOException, AlluxioException {
    try {
      mMasterClient.mergeStore(fromUri, toUri);
    } catch (UnavailableException e) {
      throw new IOException(e);
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    }
  }
}
