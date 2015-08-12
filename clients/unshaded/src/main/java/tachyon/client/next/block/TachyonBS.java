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

import tachyon.client.next.ClientOptions;

public class TachyonBS {

  public static TachyonBS get() {
    // TODO: Implement me
    return null;
  }

  public void close() {
    // TODO: Implement me
  }

  public void deleteBlock(long blockId) {
    // TODO: Implement me
  }

  public void freeBlock(long blockId) {
    // TODO: Implement me
  }

  public BlockInfo getBlockInfo(long blockId) {
    // TODO: Implement me
    return null;
  }

  public BlockInStream getBlockInStream(long blockId, ClientOptions options) {
    // TODO: Implement me
    return null;
  }

  public BlockOutStream getBlockOutStream(long blockId, ClientOptions options) {
    // TODO: Implement me
    return null;
  }

  public boolean promote(long blockId) {
    // TODO: Implement me
    return false;
  }
}
