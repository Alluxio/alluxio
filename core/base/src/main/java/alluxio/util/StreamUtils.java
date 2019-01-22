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

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Convenience methods for working with streams.
 */
public final class StreamUtils {

  /**
   * @param f the function to apply
   * @param collection a collection to map over
   * @param <T> the input type
   * @param <R> the output type
   * @return a list containing the elements of the collection with the function applied
   */
  public static <T, R> List<R> map(Function<T, R> f, Collection<T> collection) {
    return collection.stream().map(f).collect(Collectors.toList());
  }

  private StreamUtils() {} // prevent instantiation
}
