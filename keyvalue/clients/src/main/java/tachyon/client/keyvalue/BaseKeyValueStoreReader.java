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

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.thrift.PartitionInfo;
import tachyon.util.io.BufferUtils;

/**
 * Default implementation of {@link KeyValueStoreReader} to access a Tachyon key-value store.
 */
@NotThreadSafe
class BaseKeyValueStoreReader implements KeyValueStoreReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonConf mConf = ClientContext.getConf();
  private final InetSocketAddress mMasterAddress = ClientContext.getMasterAddress();
  private final KeyValueMasterClient mMasterClient;

  /** A list of partitions of the store */
  private final List<PartitionInfo> mPartitions;

  /**
   * Constructs a {@link BaseKeyValueStoreReader} instance.
   *
   * @param uri URI of the key-value store
   * @throws IOException if non-Tachyon error occurs
   * @throws TachyonException if Tachyon error occurs
   */
  BaseKeyValueStoreReader(TachyonURI uri) throws IOException, TachyonException {
    // TODO(binfan): use a thread pool to manage the client.
    LOG.info("Create KeyValueStoreReader for {}", uri);
    mMasterClient = new KeyValueMasterClient(mMasterAddress, mConf);
    mPartitions = mMasterClient.getPartitionInfo(uri);
    mMasterClient.close();
  }

  @Override
  public void close() {
  }

  @Override
  public byte[] get(byte[] key) throws IOException, TachyonException {
    ByteBuffer value = get(ByteBuffer.wrap(key));
    if (value == null) {
      return null;
    }
    return BufferUtils.newByteArrayFromByteBuffer(value);
  }

  @Override
  public ByteBuffer get(ByteBuffer key) throws IOException, TachyonException {
    Preconditions.checkNotNull(key);
    // TODO(binfan): improve the inefficient for-loop to binary search.
    for (PartitionInfo partition : mPartitions) {
      // NOTE: keyStart and keyLimit are both inclusive
      if (key.compareTo(partition.keyStart) >= 0 && key.compareTo(partition.keyLimit) <= 0) {
        long blockId = partition.blockId;
        KeyValuePartitionReader reader = KeyValuePartitionReader.Factory.create(blockId);
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
