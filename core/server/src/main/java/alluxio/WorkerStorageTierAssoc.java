/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
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
 * {@link StorageTierAssoc} for workers.
 */
@ThreadSafe
public class WorkerStorageTierAssoc extends StorageTierAssoc {

  /**
   * Creates a new instance of {@link WorkerStorageTierAssoc} using a {@link Configuration}.
   */
  public WorkerStorageTierAssoc() {
    super(PropertyKey.WORKER_TIERED_STORE_LEVELS,
        PropertyKeyFormat.WORKER_TIERED_STORE_LEVEL_ALIAS_FORMAT);
  }

  /**
   * Creates a new instance of {@link WorkerStorageTierAssoc} using a list of storage tier aliases.
   *
   * @param storageTierAliases a list of storage tier aliases
   */
  public WorkerStorageTierAssoc(List<String> storageTierAliases) {
    super(storageTierAliases);
  }
}
