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

package tachyon.worker.block.allocator;


import tachyon.worker.block.BlockMetadataManager;

/**
 * Factory of {@link Allocator} based on {@link AllocatorType}
 */
public class AllocatorFactory {
  /**
   * Create a new {@link Allocator}
   *
   * @param allocatorType AllocatorType which determines the class of allocator to create
   * @param metaManager BlockMetadataManager to pass to Allocator
   * @return the generated Allocator
   */
  public static Allocator create(AllocatorType allocatorType, BlockMetadataManager metaManager) {
    switch (allocatorType) {
      case GREEDY:
        return new GreedyAllocator(metaManager);
      case MAX_FREE:
        return new MaxFreeAllocator(metaManager);
      default:
        return new GreedyAllocator(metaManager);
    }
  }

  private AllocatorFactory() {}
}

