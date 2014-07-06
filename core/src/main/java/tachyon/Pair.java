/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

/**
 * A pair representation defined by two elements (of type First and type Second).
 * 
 * @param <First>
 *          the first element of the Pair.
 * @param <Second>
 *          the second element of the Pair.
 */
public class Pair<First, Second> {
  private First mFirst;
  private Second mSecond;

  /**
   * Constructs and initializes a Pair specified by the two input elements.
   * 
   * @param first
   *          the first element of the pair (of type First)
   * @param second
   *          the second element of the pair (of type Second)
   */
  public Pair(First first, Second second) {
    mFirst = first;
    mSecond = second;
  }

  @Override
  public synchronized boolean equals(Object o) {
    if (!(o instanceof Pair)) {
      return false;
    }
    return mFirst.equals(((Pair<?, ?>) o).getFirst())
        && mSecond.equals(((Pair<?, ?>) o).getSecond());
  }

  /**
   * @return the first element of the pair.
   */
  public First getFirst() {
    return mFirst;
  }

  /**
   * @return the second element of the pair.
   */
  public Second getSecond() {
    return mSecond;
  }

  /**
   * Set the first value.
   * 
   * @param first
   *          the value to be set.
   */
  public void setFirst(First first) {
    mFirst = first;
  }

  /**
   * Set the second value.
   * 
   * @param second
   *          the value to be set.
   */
  public void setSecond(Second second) {
    mSecond = second;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("Pair(").append(mFirst).append(",").append(mSecond)
        .append(")").toString();
  }
}