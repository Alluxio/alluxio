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
  OK(0), FAILURE(1), FAILURE_KEY_NOT_FOUND(2), FAILURE_KEY_DUPLICATED(3), FAILURE_TABLE_FULL(
      4), UNDEFINED(5);

  public final int mCode;

  /**
   * Create a cuckoo status.
   */
  CuckooStatus(int code) {
    mCode = code;
  }
}
