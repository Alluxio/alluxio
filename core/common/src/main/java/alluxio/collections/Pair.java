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

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A pair representation defined by two elements (of type First and type Second).
 *
 * @param <T1> the first element of the Pair
 * @param <T2> the second element of the Pair
 */
@NotThreadSafe
public class Pair<T1, T2> {
  private T1 mFirst;
  private T2 mSecond;

  /**
   * Constructs and initializes a Pair specified by the two input elements.
   *
   * @param first the first element of the pair (of type First)
   * @param second the second element of the pair (of type Second)
   */
  public Pair(T1 first, T2 second) {
    mFirst = first;
    mSecond = second;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof Pair<?, ?>)) {
      return false;
    }
    Pair that = (Pair) o;

    return Objects.equal(mFirst, that.mFirst)
        && Objects.equal(mSecond, that.mSecond);
  }

  /**
   * @return the first element of the pair
   */
  public T1 getFirst() {
    return mFirst;
  }

  /**
   * @return the second element of the pair
   */
  public T2 getSecond() {
    return mSecond;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mFirst, mSecond);
  }

  /**
   * Set the first value.
   *
   * @param first the value to be set
   */
  public void setFirst(T1 first) {
    mFirst = first;
  }

  /**
   * Set the second value.
   *
   * @param second the value to be set
   */
  public void setSecond(T2 second) {
    mSecond = second;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("Pair(").append(mFirst).append(",").append(mSecond)
        .append(")").toString();
  }
}
