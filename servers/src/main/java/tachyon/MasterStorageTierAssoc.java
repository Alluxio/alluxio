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

package tachyon;

import java.util.List;

import tachyon.conf.TachyonConf;

/**
 * {@link StorageTierAssoc} for masters.
 */
public class MasterStorageTierAssoc extends StorageTierAssoc {

  /**
   * @param conf the configuration for Tachyon
   */
  public MasterStorageTierAssoc(TachyonConf conf) {
    super(conf, Constants.MASTER_TIERED_STORE_GLOBAL_LEVELS,
        Constants.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS_FORMAT);
  }

  /**
   * Creates a new instance of {@link MasterStorageTierAssoc}.
   *
   * @param storageTierAliases a list of storage tier aliases
   */
  public MasterStorageTierAssoc(List<String> storageTierAliases) {
    super(storageTierAliases);
  }
}
