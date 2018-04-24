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

package alluxio.collections;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class representing the definition of an index for this {@link IndexedSet}. Each instance of
 * this class must implement the method to define how to get the value of the field chosen as
 * the index key. Users use this indexDefinition class as the parameter in all methods of
 * {@link IndexedSet} to represent an index.
 *
 * @param <T> type of objects in this {@link IndexedSet}
 */
@ThreadSafe
public abstract class IndexDefinition<T> {
  /** Whether it is a unique index. */
  //TODO(lei): change the mIsUnique to mIndexType enum
  private final boolean mIsUnique;

  /**
   * Constructs a new {@link IndexDefinition} instance.
   *
   * @param isUnique whether the index is unique. A unique index is an index where each index value
   *                 only maps to one object; A non-unique index is an index where an index value
   *                 can map to one or more objects.
   */
  public IndexDefinition(boolean isUnique) {
    mIsUnique = isUnique;
  }

  /**
   * @return whether the index requires all field values to be unique
   */
  public boolean isUnique() {
    return mIsUnique;
  }

  /**
   * Gets the value of the field that serves as index.
   *
   * @param o the instance to get the field value from
   * @return the field value
   */
  public abstract Object getFieldValue(T o);
}
