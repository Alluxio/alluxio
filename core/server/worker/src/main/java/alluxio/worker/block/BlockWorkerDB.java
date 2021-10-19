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

package alluxio.worker.block;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;

/**
 * Persistent info for worker.
 */
@NotThreadSafe
public interface BlockWorkerDB {
  /**
   * Returns a cluster id for the current worker.
   *
   * @return a cluster id
   */
  String getClusterId();

  /**
   * Set the cluster id for the current worker.
   *
   * @param clusterId the cluster id of current worker
   * @throws IOException I/O error if create or write file failed
   */
  void setClusterId(String clusterId) throws IOException;

  /**
   * Resets the worker persistence info to original state.
   * noting to do if persistence file dose not exist
   * @throws IOException I/O error if create or write file failed
   */
  void resetState() throws IOException;
}
