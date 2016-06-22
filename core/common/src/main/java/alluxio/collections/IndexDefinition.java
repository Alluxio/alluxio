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

/**
 * An class representing the definition of an index for this {@link IndexedSet}, each index for
 * this set must implement the interface to define how to get the value of the field chosen as
 * the index. Users must use the index name defined in this class as the
 * parameter in all methods of {@link IndexedSet} to represent the same index.
 *
 * @param <T> type of objects in this {@link IndexedSet}
 */
public class IndexDefinition<T> {

  /**
   * An interface abstracting the value of the field that serves as index.
   *
   * @param <T> type of objects in this {@link Abstracter}
   */
  public interface Abstracter<T> {
    /**
     * Gets the value of the field that serves as index.
     *
     * @param o the instance to get the field value from
     * @return the field value, which is just an Object
     */
    Object getFieldValue(T o);
  }

  /** The index name, representing the index in methods of {@link IndexedSet}. */
  private final String mName;

  /** Whether it is a unique index. */
  //TODO(lei): change the mIsUnique to mIndexType enum
  private final boolean mIsUnique;

  /** Abstracts the data field of this index. */
  private final Abstracter mAbstracter;

  /**
   * Constructs a new {@link IndexDefinition} instance.
   *
   * @param name the name of the field index, used as index id
   * @param isUnique if the index is unique. A unique index is an index where each index value only
   *                 maps toone object; A non-unique index is an index where an index value can map
   *                 to one or more objects.
   * @param abstracter the interface to abstract the value of the field that serves as index
   */
  public IndexDefinition(String name, boolean isUnique, Abstracter abstracter) {
    mIsUnique = isUnique;
    mAbstracter = abstracter;
    mName = name;
  }

  /**
   * @return is the index a unique one
   */
  public boolean isUnique() {
    return mIsUnique;
  }

  /**
   * @return the abstracter of the index
   */
  public Abstracter getAbstracter() {
    return mAbstracter;
  }

  /**
   * @return the name of the index
   */
  public String getName() {
    return mName;
  }
}
