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

package tachyon.client.keyvalue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.ClientContext;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.thrift.PartitionInfo;

/**
 * Implementation of {@link KeyValueStoreReader} to access a Tachyon key-value store.
 * <p>
 * This class is not thread-safe.
 */
@PublicApi
public class BaseKeyValueStoreReader implements KeyValueStoreReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonConf mConf = ClientContext.getConf();
  private final InetSocketAddress mMasterAddress = ClientContext.getMasterAddress();
  private final KeyValueMasterClient mMasterClient;

  /** A list of partitions of the store */
  private final List<PartitionInfo> mPartitions;

  /**
   * Constructs a {@link BaseKeyValueStoreReader} instance.
   * TODO(binfan): use a thread pool to manage the client.
   *
   * @param uri URI of the key-value store
   */
  protected BaseKeyValueStoreReader(TachyonURI uri) throws IOException, TachyonException {
    LOG.info("Create KeyValueStoreReader for {}", uri);
    mMasterClient = new KeyValueMasterClient(mMasterAddress, mConf);
    mPartitions = mMasterClient.getPartitionInfo(uri);
    mMasterClient.close();
  }

  @Override
  public void close() {
    // cleanup any opened clients.
  }

  @Override
  public ByteBuffer get(ByteBuffer key) throws IOException, TachyonException {
    Preconditions.checkNotNull(key);
    // TODO(binfan): improve the inefficient for-loop to binary search.
    for (PartitionInfo partition : mPartitions) {
      if (key.compareTo(partition.keyStart) >= 0 && key.compareTo(partition.keyLimit) < 0) {
        long blockId = partition.blockId;
        KeyValueFileReader reader = KeyValueFileReader.Factory.create(blockId);
        try {
          ByteBuffer value = reader.get(key);
          return value;
        } finally {
          reader.close();
        }
      }
    }
    return null;
  }
}
