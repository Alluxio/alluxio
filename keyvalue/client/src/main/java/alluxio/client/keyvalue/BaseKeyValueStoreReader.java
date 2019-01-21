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
import alluxio.exception.AlluxioException;
import alluxio.grpc.PartitionInfo;
import alluxio.master.MasterClientConfig;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link KeyValueStoreReader} to access an Alluxio key-value store.
 */
@NotThreadSafe
class BaseKeyValueStoreReader implements KeyValueStoreReader {
  private static final Logger LOG = LoggerFactory.getLogger(BaseKeyValueStoreReader.class);

  private final KeyValueMasterClient mMasterClient;

  /** A list of partitions of the store. */
  private final List<PartitionInfo> mPartitions;

  /**
   * Constructs a {@link BaseKeyValueStoreReader} instance.
   *
   * @param uri URI of the key-value store
   */
  BaseKeyValueStoreReader(AlluxioURI uri) throws IOException {
    // TODO(binfan): use a thread pool to manage the client.
    LOG.info("Create KeyValueStoreReader for {}", uri);
    mMasterClient = new KeyValueMasterClient(MasterClientConfig.defaults());
    mPartitions = mMasterClient.getPartitionInfo(uri);
    mMasterClient.close();
  }

  @Override
  public void close() {
  }

  @Override
  @Nullable
  public byte[] get(byte[] key) throws IOException, AlluxioException {
    ByteBuffer value = get(ByteBuffer.wrap(key));
    if (value == null) {
      return null;
    }
    return BufferUtils.newByteArrayFromByteBuffer(value);
  }

  @Override
  @Nullable
  public ByteBuffer get(ByteBuffer key) throws IOException, AlluxioException {
    Preconditions.checkNotNull(key, "key");
    int left = 0;
    int right = mPartitions.size();
    while (left < right) {
      int middle = (right + left) / 2;
      PartitionInfo partition = mPartitions.get(middle);
      // NOTE: keyStart and keyLimit are both inclusive
      if (key.compareTo(partition.getKeyStart().asReadOnlyByteBuffer()) < 0) {
        right = middle;
      } else if (key.compareTo(partition.getKeyLimit().asReadOnlyByteBuffer()) > 0) {
        left = middle + 1;
      } else {
        // The key is either in this partition or not in the key-value store
        long blockId = partition.getBlockId();
        try (KeyValuePartitionReader reader = KeyValuePartitionReader.Factory.create(blockId)) {
          return reader.get(key);
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
    for (PartitionInfo partition : mPartitions) {
      totalSize += partition.getKeyCount();
    }
    return totalSize;
  }
}
