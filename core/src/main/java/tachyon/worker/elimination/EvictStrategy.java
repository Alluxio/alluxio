/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker.elimination;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import tachyon.worker.hierarchy.StorageDir;

/**
 * Interface used to determine which blocks will be evicted to get enough free space requested. For
 * efficiency considerations, resources will be locked only when they are accessed during candidate
 * selection. when actually evict blocks, some blocks selected may be not allowed to evict, because
 * pin file / locked blocks information may be updated after candidate selection. it may result in
 * having to try more than one time to get enough space.
 */
public interface EvictStrategy {

  /**
   * Get storage dir for request size and also get blocks to be evicted
   * 
   * @param blockEvictionInfoList
   *          blocks to be evicted
   * @param pinList
   *          list of pinned file
   * @param requestSize
   *          size to request
   * @return index of the storage dir allocated, toEvictedBlocks also returned as output
   * @throws IOException
   */
  StorageDir getDirCandidate(List<BlockEvictionInfo> blockEvictionInfoList, Set<Integer> pinList,
      long requestSize);
}
