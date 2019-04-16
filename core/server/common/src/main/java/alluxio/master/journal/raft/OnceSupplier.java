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

package alluxio.master.journal.raft;

import java.util.function.Supplier;

/**
 * Supplier that supplies a value only once, then throws an exception if it is called again.
 *
 * @param <T> the type of the supplied value
 */
public class OnceSupplier<T> implements Supplier<T> {
  private final T mValue;

  private boolean mInit = false;

  /**
   * @param value the value to return from the once supplier
   */
  public OnceSupplier(T value) {
    mValue = value;
  }

  @Override
  public synchronized T get() {
    if (mInit) {
      throw new IllegalStateException("OnceSupplier called multiple times");
    }
    mInit = true;
    return mValue;
  }
}
