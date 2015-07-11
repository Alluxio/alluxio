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

package tachyon.worker.block.evictor;

import tachyon.worker.block.BlockMetadataManagerView;

/**
 * Factory of {@link Evictor} based on {@link EvictorType}
 */
public class EvictorFactory {
  private EvictorFactory() {}

  /**
   * Creates an {@link Evictor} instance according to {@link EvictorType}
   *
   * @param evictorType EvictorType of the Evictor to create
   * @param view BlockMetadataManagerView to pass to Evictor
   * @return the generated Evictor
   */
  public static Evictor create(EvictorType evictorType, BlockMetadataManagerView view) {
    switch (evictorType) {
      case GREEDY:
        return new GreedyEvictor(view);
      case LRU:
        return new LRUEvictor(view);
      default:
        return new GreedyEvictor(view);
    }
  }
}
