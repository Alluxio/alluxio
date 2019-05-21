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

package alluxio.resource;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Automatically increases or decreases the count when closing.
 */
public class CountResource implements Closeable {
  private final AtomicLong mCount;
  private final boolean mIncrease;

  /**
   * Creates a new instance of {@link CountResource} using the given count.
   * The count should have been initialized outside of this class, this constructor will not modify
   * the value of the count.
   *
   * @param count the count
   * @param increase if true, increase the count when closing, otherwise, decrease the count
   */
  public CountResource(AtomicLong count, boolean increase) {
    mCount = Preconditions.checkNotNull(count, "count");
    mIncrease = increase;
  }

  /**
   * Increases or decreases the count depending on the configuration in constructor.
   */
  @Override
  public void close() {
    if (mIncrease) {
      mCount.incrementAndGet();
    } else {
      mCount.decrementAndGet();
    }
  }
}
