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

package alluxio.client;

/**
 * This interface should be implemented by all Alluxio streams which are bounded at a certain
 * byte size.
 */
public interface BoundedStream {
  /**
   * @return the remaining number of bytes left in the stream, starting at the current position
   */
  long remaining();
}
