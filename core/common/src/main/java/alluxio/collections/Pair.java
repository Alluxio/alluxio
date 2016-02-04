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

package alluxio.collections;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A pair representation defined by two elements (of type First and type Second).
 *
 * @param <T1> the first element of the Pair
 * @param <T2> the second element of the Pair
 */
@NotThreadSafe
public final class Pair<T1, T2> {
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
    if (o instanceof Pair<?, ?>) {
      return ((Pair<?, ?>) o).getFirst().equals(mFirst)
          && ((Pair<?, ?>) o).getSecond().equals(mSecond);
    }
    return false;
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
    return 31 * mFirst.hashCode() + mSecond.hashCode();
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
