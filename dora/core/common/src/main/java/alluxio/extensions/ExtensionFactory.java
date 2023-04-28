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

package alluxio.extensions;

import alluxio.conf.AlluxioConfiguration;

/**
 * A factory class for creating instance of {@link T} based on configuration {@link S}.
 * @param <T> The type of instance to be created
 * @param <S> the type of configuration to be used when creating the extension
 */
public interface ExtensionFactory<T, S extends AlluxioConfiguration> {
  /**
   * Creates a new extension for the given path. An {@link IllegalArgumentException} is
   * thrown if this factory does not support extension for the given path or if the configuration
   * provided is insufficient to create an extension.
   *
   * @param path file path with scheme for which the extension will be created
   * @param conf configuration object for the extension
   * @return the new extension
   */
  T create(String path, S conf);

  /**
   * Gets whether this factory supports the given path and thus whether calling the
   * {@link #create(String, S)} can succeed for this path.
   *
   * @param path file path with scheme for which the extension will be created
   * @param conf configuration object for the extension
   * @return true if the path is supported, false otherwise
   */
  boolean supportsPath(String path, S conf);
}
