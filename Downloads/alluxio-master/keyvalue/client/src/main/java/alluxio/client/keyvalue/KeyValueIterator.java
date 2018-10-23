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

package alluxio.client.keyvalue;

import alluxio.annotation.PublicApi;
import alluxio.exception.AlluxioException;

import java.io.IOException;

/**
 * Iterator to iterate over key-value pairs in {@link KeyValueSystem} or its partitions.
 */
@PublicApi
public interface KeyValueIterator {
  /**
   * @return true if the iterator has more key-value pairs, otherwise false
   */
  boolean hasNext();

  /**
   * Throws a {@link java.util.NoSuchElementException} if there are no more pairs.
   *
   * @return the next key-value pair in the iteration
   */
  KeyValuePair next() throws IOException, AlluxioException;
}
