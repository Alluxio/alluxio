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

package alluxio.util.interfaces;

import java.io.IOException;

/**
 * A functional lambda interface that can throw an {@link IOException}.
 *
 * @param <I> the input type
 * @param <T> the output type
 */
@FunctionalInterface
public interface IOFunction<I, T> {
  /**
   *  A lambda function with an input and output.
   *
   * @param i input parameter
   * @return object of type {@code T}
   * @throws IOException
   */
  T apply(I i) throws IOException;
}
