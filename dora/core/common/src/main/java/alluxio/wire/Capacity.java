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

package alluxio.wire;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class that represents the available, free, and total capacity.
 */
@NotThreadSafe
public class Capacity {
  private long mTotal;
  private long mUsed;

  /**
   * Creates a new instance of {@link Capacity}.
   */
  public Capacity() {}

  /**
   * @return the total capacity
   */
  public long getTotal() {
    return mTotal;
  }

  /**
   * @return the used capacity
   */
  public long getUsed() {
    return mUsed;
  }

  /**
   * @param total the total capacity to use
   * @return the capacity
   */
  public Capacity setTotal(long total) {
    mTotal = total;
    return this;
  }

  /**
   * @param used the used capacity to use
   * @return the capacity
   */
  public Capacity setUsed(long used) {
    mUsed = used;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Capacity)) {
      return false;
    }
    Capacity that = (Capacity) o;
    return mTotal == that.mTotal && mUsed == that.mUsed;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mTotal, mUsed);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("total", mTotal).add("used", mUsed).toString();
  }
}
