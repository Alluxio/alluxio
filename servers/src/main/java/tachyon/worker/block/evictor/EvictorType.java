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

/**
 * Different types of {@link Evictor}.
 */
public enum EvictorType {
  /**
   * Default type which will be determined in {@link EvictorFactory} mainly used in
   * {@link tachyon.conf.TachyonConf#getEnum} as default value when get EvictorType from
   * configuration
   */
  DEFAULT,
  /**
   * Evict old blocks among several StorageDirs by LRU
   */
  LRU,
  /**
   * Evict old blocks in certain StorageDir by LRU. The main difference between PartialLRU and
   * LRU is that LRU choose old blocks among several StorageDirs until one StorageDir
   * satisfies the request space, but PartialLRU select one StorageDir with maximum free space
   * first and evict old blocks in the selected StorageDir by LRU
   */
  PARTIAL_LRU,
  /**
   * Evict blocks continually until a StorageDir has enough space
   */
  GREEDY,
}
