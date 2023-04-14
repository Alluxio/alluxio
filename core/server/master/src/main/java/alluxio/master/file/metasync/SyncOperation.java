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

package alluxio.master.file.metasync;

/**
 * The metadata sync operations.
 */
public enum SyncOperation {
  // Compared but not updated
  NOOP(0),
  CREATE(1),
  DELETE(2),
  // Deleted then created due to the changed file data
  RECREATE(3),
  // Metadata updated
  UPDATE(4),
  SKIPPED_DUE_TO_CONCURRENT_MODIFICATION(5),
  SKIPPED_ON_MOUNT_POINT(6);

  private final int mValue;

  SyncOperation(int value) {
    mValue = value;
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
}
