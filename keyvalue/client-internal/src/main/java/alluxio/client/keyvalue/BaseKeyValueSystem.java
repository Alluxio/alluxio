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

package alluxio.client.keyvalue;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

import alluxio.TachyonURI;
import alluxio.annotation.PublicApi;
import alluxio.client.ClientContext;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.TachyonException;

/**
 * Default implementation of the {@link KeyValueSystem} interface. Developers can extend this class
 * instead of implementing the interface. This implementation reads and writes key-value data
 * through {@link BaseKeyValueStoreReader} and {@link BaseKeyValueStoreWriter}.
 */
@PublicApi
@ThreadSafe
public class BaseKeyValueSystem implements KeyValueSystem {
  private final KeyValueMasterClient mMasterClient =
      new KeyValueMasterClient(ClientContext.getMasterAddress(), ClientContext.getConf());

  @Override
  public KeyValueStoreReader openStore(TachyonURI uri) throws IOException, TachyonException {
    Preconditions.checkNotNull(uri, PreconditionMessage.URI_KEY_VALUE_STORE_NULL);
    return new BaseKeyValueStoreReader(uri);
  }

  @Override
  public KeyValueStoreWriter createStore(TachyonURI uri) throws IOException, TachyonException {
    Preconditions.checkNotNull(uri, PreconditionMessage.URI_KEY_VALUE_STORE_NULL);
    return new BaseKeyValueStoreWriter(uri);
  }

  @Override
  public void deleteStore(TachyonURI uri)
      throws IOException, InvalidPathException, FileDoesNotExistException, TachyonException {
    mMasterClient.deleteStore(uri);
  }

  @Override
  public void mergeStore(TachyonURI fromUri, TachyonURI toUri)
      throws IOException, TachyonException {
    mMasterClient.mergeStore(fromUri, toUri);
  }
}
