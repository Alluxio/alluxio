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

package alluxio.common;

/**
 * Interface for keys used to present an object which can be used with
 * {@link alluxio.client.file.dora.WorkerLocationPolicy}.
 */
public interface ShardKey {

  /**
   * @return the string representation of the key
   */
  String asString();
}
