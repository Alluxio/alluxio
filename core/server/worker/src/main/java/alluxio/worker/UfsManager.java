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

package alluxio.worker;

import alluxio.underfs.UnderFileSystem;

import java.io.Closeable;
import java.io.IOException;

/**
 * A class manages the ufs used by different worker services.
 */
public interface UfsManager extends Closeable {
  /**
   * Gets the properties for the given the ufs id. If this ufs id is new to this worker, this method
   * will query master to get the corresponding ufs info.
   *
   * @param id ufs id
   * @return the configuration of the UFS
   * @throws IOException if the file persistence fails
   */
  UnderFileSystem getUfsById(long id) throws IOException;
}
