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

import com.google.common.base.Preconditions;

import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.block.TachyonBlockStore;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.exception.TachyonException;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.NetAddress;
import tachyon.worker.keyvalue.KeyValueWorkerClient;

/**
 * A client to talk to remote key-value worker to access a key
 */
public class ClientKeyValueFileReader implements KeyValueFileReader {
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

  @Override
  public byte[] get(byte[] key) throws IOException, TachyonException {
    ByteBuffer value = mClient.get(mBlockId, ByteBuffer.wrap(key));
    if (value == null) {
      return null;
    }
    return value.array();
  }
}
