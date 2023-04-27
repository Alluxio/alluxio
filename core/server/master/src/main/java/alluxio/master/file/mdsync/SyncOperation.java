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

package alluxio.master.file.mdsync;

import com.codahale.metrics.Counter;

/**
 * The metadata sync operations.
 */
public enum SyncOperation {
  // Compared but not updated
  NOOP(0, SyncOperationMetrics.NOOP_COUNT),
  CREATE(1, SyncOperationMetrics.CREATE_COUNT),
  DELETE(2, SyncOperationMetrics.DELETE_COUNT),
  // Deleted then created due to the changed file data
  RECREATE(3, SyncOperationMetrics.RECREATED_COUNT),
  // Metadata updated
  UPDATE(4, SyncOperationMetrics.UPDATE_COUNT),
  SKIPPED_DUE_TO_CONCURRENT_MODIFICATION(5, SyncOperationMetrics.SKIP_CONCURRENT_UPDATE_COUNT),
  SKIPPED_ON_MOUNT_POINT(6, SyncOperationMetrics.SKIP_MOUNT_POINT_COUNT),
  SKIPPED_NON_PERSISTED(7, SyncOperationMetrics.SKIPPED_NON_PERSISTED_COUNT);

  private final int mValue;
  private final Counter mCounter;

  SyncOperation(int value, Counter counter) {
    mValue = value;
    mCounter = counter;
  }

  /**
   * @param value the value
   * @return the enum of the value
   */
  public static SyncOperation fromInteger(int value) {
    switch (value) {
      case 0:
        return NOOP;
      case 1:
        return CREATE;
      case 2:
        return DELETE;
      case 3:
        return RECREATE;
      case 4:
        return UPDATE;
      case 5:
        return SKIPPED_DUE_TO_CONCURRENT_MODIFICATION;
      case 6:
        return SKIPPED_ON_MOUNT_POINT;
      case 7:
        return SKIPPED_NON_PERSISTED;
      default:
        throw new IllegalArgumentException("Invalid SyncOperation value: " + value);
    }
  }

  /**
   * @return the value
   */
  public int getValue() {
    return mValue;
  }

  /**
   * @return the metric counter
   */
  public Counter getCounter() {
    return mCounter;
  }
}
