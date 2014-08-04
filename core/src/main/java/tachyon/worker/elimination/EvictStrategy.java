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

/**
 * interface used to determine which blocks will be evicted to get enough free space requested.
 */
public interface EvictStrategy {

  /**
   * get storage dir for request size and also get blocks to be evicted
   * 
   * @param blockEvictionInfoList
   *          blocks to be evicted
   * @param pinList
   *          list of pinned file
   * @param lastTier
   *          whether current storage tier is the last tier
   * @param requestSize
   *          size to request
   * @return index of the storage dir allocated, toEvictedBlocks also returned as output
   * @throws IOException
   */
  int getDirCandidate(List<BlockEvictionInfo> blockEvictionInfoList, Set<Integer> pinList,
      boolean lastTier, long requestSize) throws IOException;
}
