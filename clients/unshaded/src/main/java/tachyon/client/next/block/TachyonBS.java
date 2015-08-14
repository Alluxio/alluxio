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

import java.io.Closeable;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.next.ClientOptions;
import tachyon.conf.TachyonConf;

/**
 * Tachyon Block Store client. This is an internal client for all block level operations in
 * Tachyon. An instance of this class can be obtained via {@link TachyonBS#get}. This class
 * is thread safe.
 */
public class TachyonBS implements Closeable {

  public static TachyonBS sCachedClient = null;

  public static synchronized TachyonBS get(TachyonConf conf) {
    if (sCachedClient != null) {
      return sCachedClient;
    }
    sCachedClient = new TachyonBS(conf);
    return sCachedClient;
  }

  private BlockMasterClientPool mMasterClientPool;

  public TachyonBS(TachyonConf conf) {
    // TODO: Fix the default
    TachyonURI masterURI = new TachyonURI(conf.get(Constants.MASTER_ADDRESS, ""));
  }

  public void close() {
    // TODO: Implement me
  }

  public void delete(long blockId) {
    // TODO: Implement me
  }

  public void free(long blockId) {
    // TODO: Implement me
  }

  public BlockInfo getInfo(long blockId) {
    // TODO: Implement me
    return null;
  }

  public BlockInStream getInStream(long blockId, ClientOptions options) {
    // TODO: Implement me
    return null;
  }

  public BlockOutStream getOutStream(long blockId, ClientOptions options) {
    // TODO: Implement me
    return null;
  }

  public boolean promote(long blockId) {
    // TODO: Implement me
    return false;
  }
}
