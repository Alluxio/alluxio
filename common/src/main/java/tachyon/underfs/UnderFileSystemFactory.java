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

package tachyon.underfs;

import tachyon.conf.TachyonConf;

/**
 * Interface for under file system factories
 */
public interface UnderFileSystemFactory {

  /**
   * Creates a new client for accessing the given path
   *
   * @param path File path
   * @param tachyonConf Tachyon configuration
   * @param ufsConf Optional configuration object for the UFS, may be null
   * @return Client
   * @throws IllegalArgumentException Thrown if this factory does not support clients for the given
   *         path or if the configuration provided is insufficient to create a client
   */
  UnderFileSystem create(String path, TachyonConf tachyonConf, Object ufsConf);

  /**
   * Gets whether this factory supports the given path and thus whether calling the
   * {@link #create(String, TachyonConf, Object)} can succeed for this path
   *
   * @param path File path
   * @param tachyonConf Tachyon configuration
   * @return True if the path is supported, false otherwise
   */
  boolean supportsPath(String path, TachyonConf tachyonConf);
}
