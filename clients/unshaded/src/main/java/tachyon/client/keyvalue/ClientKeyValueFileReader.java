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
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.block.TachyonBlockStore;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.exception.TachyonException;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.io.BufferUtils;
import tachyon.worker.keyvalue.KeyValueWorkerClient;

/**
 * A client to talk to remote key-value worker to access a key
 */
public class ClientKeyValueFileReader implements KeyValueFileReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private KeyValueWorkerClient mClient;
  private long mBlockId;

  public ClientKeyValueFileReader(TachyonURI uri) throws TachyonException, IOException {
    Preconditions.checkArgument(uri != null);
    TachyonFileSystem tfs = TachyonFileSystem.TachyonFileSystemFactory.get();
    TachyonFile tFile = tfs.open(uri);
    List<Long> blockIds = tfs.getInfo(tFile).getBlockIds();
    mBlockId = blockIds.get(0);
    BlockInfo info = TachyonBlockStore.get().getInfo(mBlockId);
    NetAddress workerAddr = info.getLocations().get(0).getWorkerAddress();
    mClient = new KeyValueWorkerClient(workerAddr, ClientContext.getConf());
  }

  // This could be slow when value size is large, use in cautious.
  @Override
  public byte[] get(byte[] key) throws IOException, TachyonException {
    ByteBuffer keyBuffer = ByteBuffer.wrap(key);
    ByteBuffer value = get(keyBuffer);
    if (value == null) {
      return null;
    }
    return BufferUtils.newByteArrayFromByteBuffer(value);
  }

  @Override
  public ByteBuffer get(ByteBuffer key) throws IOException, TachyonException {
    LOG.debug("get key of length: {}", key.limit());
    return mClient.get(mBlockId, key);
  }

  @Override
  public void close() {
    mClient.close();
  }
}
