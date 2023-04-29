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

package alluxio.util;

import java.util.Iterator;

/**
 * Util for iterators.
 */
public class IteratorUtils {
  /**
   * @param iterator the iterator
   * @return the next element in the iterator or null if hasNext() returns false
   * @param <T> the type of elements returned by the iterator
   */
  public static <T> T nextOrNull(Iterator<T> iterator) {
    if (iterator.hasNext()) {
      return iterator.next();
    }
    return null;
  }
}

