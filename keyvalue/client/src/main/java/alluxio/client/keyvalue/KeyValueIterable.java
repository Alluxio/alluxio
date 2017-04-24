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
 * Interface to be implemented in classes that support iterating over key-value pairs.
 *
 * TODO(cc): Try to get rid of KeyValueIterable and KeyValueIterator when AlluxioException becomes
 * a subclass of IOException.
 */
@PublicApi
public interface KeyValueIterable {
  /**
   * @return a {@link KeyValueIterator} for iterating over key-value pairs in the store
   */
  KeyValueIterator iterator() throws IOException, AlluxioException;
}
