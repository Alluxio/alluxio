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

package alluxio.worker.block;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A read-write lock to guard one block. There should be only one lock per block.
 */
@ThreadSafe
public enum BlockLockType {
  READ(0), // A read lock
  WRITE(1), // A write lock
  ;

  private final int mValue;

  BlockLockType(int value) {
    mValue = value;
  }

  /**
   * @return the value
   */
  public int getValue() {
    return mValue;
  }
}
