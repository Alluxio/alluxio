/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.keyvalue;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.exception.AlluxioException;
import alluxio.thrift.PartitionInfo;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Default implementation of {@link KeyValueStoreReader} to access an Alluxio key-value store.
 */
@NotThreadSafe
class BaseKeyValueStoreReader implements KeyValueStoreReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Configuration mConf = ClientContext.getConf();
  private final InetSocketAddress mMasterAddress = ClientContext.getMasterAddress();
  private final KeyValueMasterClient mMasterClient;

  /** A list of partitions of the store. */
  private final List<PartitionInfo> mPartitions;

  /**
   * Constructs a {@link BaseKeyValueStoreReader} instance.
   *
   * @param uri URI of the key-value store
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if Alluxio error occurs
   */
  BaseKeyValueStoreReader(AlluxioURI uri) throws IOException, AlluxioException {
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
  public byte[] get(byte[] key) throws IOException, AlluxioException {
    ByteBuffer value = get(ByteBuffer.wrap(key));
    if (value == null) {
      return null;
    }
    return BufferUtils.newByteArrayFromByteBuffer(value);
  }

  @Override
  public ByteBuffer get(ByteBuffer key) throws IOException, AlluxioException {
    Preconditions.checkNotNull(key);
    int left = 0;
    int right = mPartitions.size();
    while (left < right) {
      int middle = (right + left) / 2;
      PartitionInfo partition = mPartitions.get(middle);
      // NOTE: keyStart and keyLimit are both inclusive
      if (key.compareTo(partition.bufferForKeyStart()) < 0) {
        right = middle;
      } else if (key.compareTo(partition.bufferForKeyLimit()) > 0) {
        left = middle + 1;
      } else {
        // The key is either in this partition or not in the kv store
        long blockId = partition.getBlockId();
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

  @Override
  public KeyValueIterator iterator() throws IOException, AlluxioException {
    return new KeyValueStoreIterator(mPartitions);
  }

  @Override
  public int size() throws IOException, AlluxioException {
    int totalSize = 0;
    // TODO(cc): Put size into PartitionInfo.
    for (PartitionInfo partition : mPartitions) {
      KeyValuePartitionReader partitionReader =
          KeyValuePartitionReader.Factory.create(partition.getBlockId());
      totalSize += partitionReader.size();
      partitionReader.close();
    }
    return totalSize;
  }
}
