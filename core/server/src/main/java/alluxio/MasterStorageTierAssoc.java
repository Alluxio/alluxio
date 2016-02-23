/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link StorageTierAssoc} for masters.
 */
@ThreadSafe
public class MasterStorageTierAssoc extends StorageTierAssoc {

  /**
   * @param conf the configuration for Alluxio
   */
  public MasterStorageTierAssoc(Configuration conf) {
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
