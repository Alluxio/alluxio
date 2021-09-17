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

package alluxio.master.block.meta;

import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An object representation of fields relevant to worker usage and capacity.
 * This class is not thread safe so external locking is required.
 * You should lock externally with {@link MasterWorkerInfo#lock(EnumSet, boolean)}
 * with {@link WorkerMetaLockSection#USAGE} specified.
 */
@NotThreadSafe
public class WorkerUsageMeta {
  /** Capacity of worker in bytes. */
  long mCapacityBytes;
  /** Worker's used bytes. */
  long mUsedBytes;
  /** Worker-specific mapping between storage tier alias and storage tier ordinal. */
  StorageTierAssoc mStorageTierAssoc;
  /** Mapping from storage tier alias to total bytes. */
  Map<String, Long> mTotalBytesOnTiers;
  /** Mapping from storage tier alias to used bytes. */
  Map<String, Long> mUsedBytesOnTiers;
  /** Mapping from tier alias to lost storage paths. */
  Map<String, List<String>> mLostStorage;

  /**
   * Constructor.
   */
  public WorkerUsageMeta() {
    mStorageTierAssoc = null;
    mTotalBytesOnTiers = new HashMap<>();
    mUsedBytesOnTiers = new HashMap<>();
    mLostStorage = new HashMap<>();
  }

  /**
   * Update the worker resource usage. An exclusive lock is required.
   *
   * Example:
   * <blockquote><pre>
   *   EnumSet<WorkerMetaLockSection> lockTypes =
   *       EnumSet.of(WorkerMetaLockSection.USAGE_LOCK);
   *   worker.lock(lockTypes, false);
   *   try {
   *     updateUsage(..);
   *   } finally {
   *     worker.unlock(lockTypes, false);
   *   }
   * </pre></blockquote>
   */
  void updateUsage(final StorageTierAssoc globalStorageTierAssoc,
                   final List<String> storageTierAliases,
                   final Map<String, Long> totalBytesOnTiers,
                   final Map<String, Long> usedBytesOnTiers)
      throws IllegalArgumentException {
    // If the storage aliases do not have strictly increasing ordinal value based on the total
    // ordering, throw an error
    for (int i = 0; i < storageTierAliases.size() - 1; i++) {
      if (globalStorageTierAssoc.getOrdinal(storageTierAliases.get(i)) >= globalStorageTierAssoc
              .getOrdinal(storageTierAliases.get(i + 1))) {
        throw new IllegalArgumentException(
                "Worker cannot place storage tier " + storageTierAliases.get(i) + " above "
                        + storageTierAliases.get(i + 1) + " in the hierarchy");
      }
    }

    WorkerStorageTierAssoc storageTierAssoc = new WorkerStorageTierAssoc(storageTierAliases);
    // validate the number of tiers
    if (storageTierAssoc.size() != totalBytesOnTiers.size()
            || storageTierAssoc.size() != usedBytesOnTiers.size()) {
      throw new IllegalArgumentException(
          "totalBytesOnTiers and usedBytesOnTiers should have the same number of tiers as "
              + "storageTierAliases, but storageTierAliases has " + storageTierAssoc.size()
              + " tiers, while totalBytesOnTiers has " + totalBytesOnTiers.size()
              + " tiers and usedBytesOnTiers has " + usedBytesOnTiers.size() + " tiers");
    }

    mStorageTierAssoc = storageTierAssoc;
    // defensive copy
    mTotalBytesOnTiers = new HashMap<>(totalBytesOnTiers);
    mUsedBytesOnTiers = new HashMap<>(usedBytesOnTiers);
    mCapacityBytes = 0;
    for (long bytes : mTotalBytesOnTiers.values()) {
      mCapacityBytes += bytes;
    }
    mUsedBytes = 0;
    for (long bytes : mUsedBytesOnTiers.values()) {
      mUsedBytes += bytes;
    }
  }

  /**
   * A shared lock is required.
   *
   * Example:
   * <blockquote><pre>
   *   EnumSet<WorkerMetaLockSection> lockTypes =
   *       EnumSet.of(WorkerMetaLockSection.USAGE_LOCK);
   *   worker.lock(lockTypes, true);
   *   try {
   *     getAvailableBytes();
   *   } finally {
   *     worker.unlock(lockTypes, true);
   *   }
   * </pre></blockquote>
   *
   * @return the available space of the worker in bytes
   */
  long getAvailableBytes() {
    return mCapacityBytes - mUsedBytes;
  }
}
