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
import java.nio.ByteBuffer;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.client.ClientContext;
import alluxio.client.file.FileSystem;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.AlluxioException;
import alluxio.thrift.PartitionInfo;
import alluxio.util.io.BufferUtils;

/**
 * Default implementation of {@link KeyValueStoreWriter} to create a Alluxio key-value store.
 */
@NotThreadSafe
class BaseKeyValueStoreWriter implements KeyValueStoreWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final FileSystem mFileSystem = FileSystem.Factory.get();
  private final KeyValueMasterClient mMasterClient;
  private final AlluxioURI mStoreUri;

  private long mPartitionIndex;
  private KeyValuePartitionWriter mWriter = null;
  /** min key in the current partition */
  private ByteBuffer mKeyStart = null;
  /** max key in the current partition */
  private ByteBuffer mKeyLimit = null;
  /** whether this writer is closed */
  private boolean mClosed;
  /** whether this writer is canceled */
  private boolean mCanceled;

  /**
   * Constructs a {@link BaseKeyValueStoreWriter}. This constructor will create a new key-value
   * store at the given {@link AlluxioURI}.
   *
   * @param uri URI of the store
   * @throws IOException if a non-Alluxio exception occurs
   * @throws AlluxioException if an unexpected Alluxio exception is thrown
   */
  BaseKeyValueStoreWriter(AlluxioURI uri) throws IOException, AlluxioException {
    LOG.info("Create KeyValueStoreWriter for {}", uri);
    mMasterClient =
        new KeyValueMasterClient(ClientContext.getMasterAddress(), ClientContext.getConf());

    mStoreUri = Preconditions.checkNotNull(uri);
    mMasterClient.createStore(mStoreUri);
    mPartitionIndex = 0;
    mClosed = false;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      if (mCanceled) {
        mWriter.cancel();
        // TODO(binfan): cancel all other written partitions
      } else {
        completePartition();
        mMasterClient.completeStore(mStoreUri);
      }
    } catch (AlluxioException e) {
      throw new IOException(e);
    } finally {
      mMasterClient.close();
    }
    mClosed = true;
  }

  @Override
  public void cancel() throws IOException {
    mCanceled = true;
    close();
  }

  @Override
  public void put(byte[] key, byte[] value) throws IOException, AlluxioException {
    Preconditions.checkNotNull(key, PreconditionMessage.ERR_PUT_NULL_KEY);
    Preconditions.checkNotNull(value, PreconditionMessage.ERR_PUT_NULL_KEY);
    Preconditions.checkArgument(key.length > 0, PreconditionMessage.ERR_PUT_EMPTY_KEY);
    Preconditions.checkArgument(value.length > 0, PreconditionMessage.ERR_PUT_EMPTY_VALUE);

    // If this is the first put to the first partition in this store, create a new partition; or
    // if this is a put to an existing but full partition, create a new partition and switch to
    // this one.
    if (mWriter == null || !mWriter.canPut(key, value)) {
      // Need to save the existing partition before switching to the next partition.
      if (mWriter != null) {
        completePartition();
      }
      mWriter = KeyValuePartitionWriter.Factory.create(getPartitionName());
      mKeyStart = null;
      mKeyLimit = null;
    }

    // If we are still unable to put this key-value pair after switching partition, throw exception.
    if (!mWriter.canPut(key, value)) {
      throw new IOException(ExceptionMessage.KEY_VALUE_TOO_LARGE
          .getMessage(key.length, value.length));
    }

    mWriter.put(key, value);
    ByteBuffer keyBuf = ByteBuffer.wrap(key);
    // Update the min key in the current partition.
    if (mKeyStart == null || keyBuf.compareTo(mKeyStart) < 0) {
      mKeyStart = ByteBuffer.allocate(key.length);
      mKeyStart.put(key);
      mKeyStart.flip();
    }
    // Update the max key in the current partition.
    if (mKeyLimit == null || keyBuf.compareTo(mKeyLimit) > 0) {
      mKeyLimit = ByteBuffer.allocate(key.length);
      mKeyLimit.put(key);
      mKeyLimit.flip();
    }
  }

  @Override
  public void put(ByteBuffer key, ByteBuffer value) throws IOException, AlluxioException {
    Preconditions.checkNotNull(key, PreconditionMessage.ERR_PUT_NULL_KEY);
    Preconditions.checkNotNull(value, PreconditionMessage.ERR_PUT_NULL_VALUE);
    // TODO(binfan): make efficient implementation
    byte[] keyArray = BufferUtils.newByteArrayFromByteBuffer(key);
    byte[] valueArray = BufferUtils.newByteArrayFromByteBuffer(value);
    put(keyArray, valueArray);
  }

  /**
   * @return {@link AlluxioURI} to the current partition file
   */
  private AlluxioURI getPartitionName() {
    return new AlluxioURI(String.format("%s/part-%05d", mStoreUri, mPartitionIndex));
  }

  /**
   * Completes the current partition.
   *
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if Alluxio error occurs
   */
  private void completePartition() throws IOException, AlluxioException {
    if (mWriter == null) {
      return;
    }
    mWriter.close();
    List<Long> blockIds = mFileSystem.getStatus(getPartitionName()).getBlockIds();
    long blockId = blockIds.get(0);
    PartitionInfo info = new PartitionInfo(mKeyStart, mKeyLimit, blockId);
    mMasterClient.completePartition(mStoreUri, info);
    mPartitionIndex ++;
  }
}
