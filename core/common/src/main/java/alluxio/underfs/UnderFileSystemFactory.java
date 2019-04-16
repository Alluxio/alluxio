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

package alluxio.underfs;

import alluxio.annotation.PublicApi;
import alluxio.conf.AlluxioConfiguration;
import alluxio.extensions.ExtensionFactory;

import javax.annotation.Nullable;

/**
 * Interface for under file system factories.
 */
@PublicApi
public interface UnderFileSystemFactory
    extends ExtensionFactory<UnderFileSystem, UnderFileSystemConfiguration> {

  /**
   * Creates a new client for accessing the given path. An {@link IllegalArgumentException} is
   * thrown if this factory does not support clients for the given path or if the configuration
   * provided is insufficient to create a client.
   *
   * @param path file path
   * @param conf optional configuration object for the UFS, may be null
   * @param alluxioConf configuration object for alluxio
   * @return the client
   */
  UnderFileSystem create(String path, @Nullable UnderFileSystemConfiguration conf,
      AlluxioConfiguration alluxioConf);

  /**
   * Gets whether this factory supports the given path and thus whether calling the
   * {@link #create(String, UnderFileSystemConfiguration, AlluxioConfiguration)} can succeed for
   * this path.
   *
   * @param path file path
   * @return true if the path is supported, false otherwise
   */
  boolean supportsPath(String path);

  /**
   * Gets whether this factory supports the given path and thus whether calling the
   * {@link #create(String, UnderFileSystemConfiguration, AlluxioConfiguration)} can succeed for
   * this path.
   *
   * @param path file path
   * @param conf optional configuration object for the UFS, may be null
   * @return true if the path is supported, false otherwise
   */
  default boolean supportsPath(String path, @Nullable UnderFileSystemConfiguration conf) {
    return supportsPath(path);
  }

  /**
   * Get the version supported by this factory.
   *
   * @return the version string
   */
  default String getVersion() {
    return "";
  }
}
