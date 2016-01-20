/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.keyvalue;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

/**
 * A key-value pair.
 */
public class KeyValuePair implements Comparable<KeyValuePair> {
  private ByteBuffer mKey;
  private ByteBuffer mValue;

  /**
   * Constructs a key-value pair. Direct references of key or value is passed in as class members,
   * no copy happens.
   *
   * @param key the key, cannot be null
   * @param value the value, cannot be null
   */
  public KeyValuePair(ByteBuffer key, ByteBuffer value) {
    Preconditions.checkNotNull(key, "key in KeyValuePair cannot be null");
    Preconditions.checkNotNull(value, "value in KeyValuePair cannot be null");

    mKey = key;
    mValue = value;
  }

  /**
   * Constructs a key-value pair. The key or value is directly wrapped into the corresponding
   * internal{@link ByteBuffer}, no copy happens.
   *
   * @param key the key, cannot be null
   * @param value the value, cannot be null
   */
  public KeyValuePair(byte[] key, byte[] value) {
    Preconditions.checkNotNull(key, "key in KeyValuePair cannot be null");
    Preconditions.checkNotNull(value, "value in KeyValuePair cannot be null");

    mKey = ByteBuffer.wrap(key);
    mValue = ByteBuffer.wrap(value);
  }

  /**
   * @return the direct reference of the internal key, no copy happens
   */
  public ByteBuffer getKey() {
    return mKey;
  }

  /**
   * @return the direct reference of the internal value, no copy happens
   */
  public ByteBuffer getValue() {
    return mValue;
  }

  /**
   * Compares this key-value pair to another.
   * <p>
   * Two pairs are compared by comparing their key {@link ByteBuffer}.
   *
   * @param o the object to be compared, cannot be null
   * @return A negative integer, zero, or a positive integer as the key of this pair is less than,
   *    equal to, or greater than that of the given pair
   */
  @Override
  public int compareTo(KeyValuePair o) {
    Preconditions.checkNotNull(o, "The given KeyValuePair cannot be null");
    return mKey.compareTo(o.getKey());
  }

  /**
   * Tells whether or not this key-value pair is equal to another object.
   * <p>
   * Two key-value pairs are equal if, and only if, their key and value {@link ByteBuffer}s are
   * equal to each other.
   *
   * @param o the object to which this pair is to be compared
   * @return true if, and only if, this pair is equal to the given object
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KeyValuePair)) {
      return false;
    }
    KeyValuePair that = (KeyValuePair) o;
    return mKey.equals(that.getKey()) && mValue.equals(that.getValue());
  }

  @Override
  public int hashCode() {
    return 31 * mKey.hashCode() + mValue.hashCode();
  }
}
