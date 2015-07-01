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

package tachyon.worker.block;

/**
 * Interface for listening on meta data mutation methods of {@link TieredBlockStore}. Methods may be
 * called concurrently, and listener implementation needs to ensure thread safety.
 */
public interface BlockStoreEventListener {

  void onAccessBlock(long userId, long blockId);

  void onCommitBlock(long userId, long blockId, BlockStoreLocation location);

  void onAbortBlock(long userId, long blockId);

  void onMoveBlockByClient(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation);

  void onRemoveBlockByClient(long userId, long blockId);

  void onMoveBlockByWorker(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation);

  void onRemoveBlockByWorker(long userId, long blockId);

}
