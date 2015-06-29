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
 * Interface for listening on meta data mutation methods of {@link TieredBlockStore}.
 */
public interface BlockMetaEventListener {

  void preCommitBlock(long userId, long blockId, BlockStoreLocation location);

  void postCommitBlock(long userId, long blockId, BlockStoreLocation location);

  void preAbortBlock(long userId, long blockId);

  void postAbortBlock(long userId, long blockId);

  void preMoveBlockByClient(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation);

  void postMoveBlockByClient(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation);

  void preRemoveBlockByClient(long userId, long blockId);

  void postRemoveBlockByClient(long userId, long blockId);

  void preMoveBlockByWorker(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation);

  void postMoveBlockByWorker(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation);

  void preRemoveBlockByWorker(long userId, long blockId);

  void postRemoveBlockByWorker(long userId, long blockId);

}
