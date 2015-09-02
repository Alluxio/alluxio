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

package tachyon.client.next.block;

import java.io.IOException;
import java.io.InputStream;

import tachyon.client.next.ClientContext;
import tachyon.client.next.InStream;
import tachyon.thrift.NetAddress;
import tachyon.util.network.NetworkAddressUtils;

/**
 * Provides a stream API to read a block from Tachyon. An instance extending this class can be
 * obtained by calling {@link TachyonBlockStore#getInStream}. Multiple BlockInStreams can be opened
 * for a block. This class is not thread safe and should only be used by one thread.
 *
 * This class provides the same methods as a Java {@link InputStream} with an additional seek
 * method.
 */
public abstract class BlockInStream extends InStream {
  public static BlockInStream get(long blockId, long blockSize, NetAddress location)
      throws IOException {
    String localHostname = NetworkAddressUtils.getLocalHostName(ClientContext.getConf());
    if (location.getMHost().equals(localHostname)) {
      return new LocalBlockInStream(blockId);
    } else {
      return new RemoteBlockInStream(blockId, blockSize, location);
    }
  }
}
