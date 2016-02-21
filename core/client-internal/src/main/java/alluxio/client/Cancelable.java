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

package alluxio.client;

import java.io.IOException;

/**
 * This interface should be implemented by all Alluxio output streams which support aborting the
 * temporary data that has been written.
 */
public interface Cancelable {
  /**
   * Cancels the write to Alluxio storage. This will delete all the temporary data and metadata
   * that has been written to the worker(s). This method should be called when a write is aborted.
   *
   * @throws IOException if there is a failure when the worker invalidates the cache attempt
   */
  void cancel() throws IOException;
}
