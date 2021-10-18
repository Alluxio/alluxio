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

package alluxio.client.file.cache.cuckoofilter;

/**
 * This class represents the status of tag position.
 */
enum CuckooStatus {
  /**
   * The status indicates success.
   */
  OK(0),
  /**
   * The status indicates failure.
   */
  FAILURE(1),
  /**
   * The status indicates failure, for the reason of key not found.
   */
  FAILURE_KEY_NOT_FOUND(2),
  /**
   * The status indicates failure, for the reason of key duplicated.
   */
  FAILURE_KEY_DUPLICATED(3),
  /**
   * The status indicates failure, for the reason of table full.
   */
  FAILURE_TABLE_FULL(4),
  /**
   * The status indicates an undefined state.
   */
  UNDEFINED(5);

  public final int mCode;

  /**
   * Create a cuckoo status.
   */
  CuckooStatus(int code) {
    mCode = code;
  }

  /**
   * @return the type of status
   */
  public int getCode() {
    return mCode;
  }
}
