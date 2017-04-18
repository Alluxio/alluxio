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

import java.io.IOException;

/**
 * Similar to {@link Cancelable}, but the cancel method doesn't throw {@link IOException}.
 */
public interface QuietlyCancelable {
  /**
   * Cancels the write to Alluxio storage. This will delete all the temporary data and metadata
   * that has been written to the worker(s). This method should be called when a write is aborted.
   */
  void cancel();
}
