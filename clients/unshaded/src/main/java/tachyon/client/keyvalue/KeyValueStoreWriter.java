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

import java.io.Closeable;
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
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.TachyonFileSystem.TachyonFileSystemFactory;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.thrift.PartitionInfo;

/**
 * Writer to create a Tachyon key-value store.
 */
@PublicApi
public class KeyValueStoreWriter implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonFileSystem mTfs = TachyonFileSystemFactory.get();
  private final TachyonConf mConf = ClientContext.getConf();
  private final InetSocketAddress mMasterAddress = ClientContext.getMasterAddress();
  private final KeyValueMasterClient mMasterClient;
  private final TachyonURI mStoreUri;

  private long mPartitionIndex;
  private KeyValueFileWriter mWriter = null;
  private ByteBuffer mKeyStart = null;
  private ByteBuffer mKeyLimit = null;

  /**
   * Factory method to create a {@link KeyValueStoreReader}.
   *
   * @param uri URI of the store
   */
  public KeyValueStoreWriter(TachyonURI uri) throws IOException, TachyonException {
    LOG.debug("Create KeyValueStoreReader for {}", uri);
    mMasterClient = new KeyValueMasterClient(mMasterAddress, mConf);

    mStoreUri = Preconditions.checkNotNull(uri);
    mTfs.mkdir(mStoreUri);
    mMasterClient.createStore(mStoreUri);
    mPartitionIndex = 0;
  }

  @Override
  public void close() throws IOException {
    try {
      mMasterClient.completeStore(mStoreUri);
      mMasterClient.close();
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }

  public void put(byte[] key, byte[] value) throws IOException, TachyonException {
    if (mWriter == null || mWriter.isFull()) { // Need to switch to the next partition.
      if (mWriter != null) {
        completePartition();
      }
      mWriter = KeyValueFileWriter.Factory.create(getPartitionName());
    }
    mWriter.put(key, value);
    ByteBuffer keyBuf = ByteBuffer.wrap(key);
    if (mKeyStart == null || keyBuf.compareTo(mKeyStart) < 0) {
      mKeyStart = ByteBuffer.allocate(key.length);
      mKeyStart.put(key);
    }
    if (mKeyLimit == null || keyBuf.compareTo(mKeyLimit) > 0) {
      mKeyLimit = ByteBuffer.allocate(key.length);
      mKeyLimit.put(key);
    }
  }

  private TachyonURI getPartitionName() {
    return new TachyonURI(String.format("%s/part-%d", mStoreUri, mPartitionIndex));
  }

  private void completePartition() throws IOException, TachyonException {
    mWriter.close();
    TachyonFile tFile = mTfs.open(getPartitionName());
    List<Long> blockIds = mTfs.getInfo(tFile).getBlockIds();
    long blockId = blockIds.get(0);
    PartitionInfo info = new PartitionInfo(mKeyStart, mKeyLimit, blockId);
    mMasterClient.completePartition(mStoreUri, info);
    mPartitionIndex ++;
  }
}
