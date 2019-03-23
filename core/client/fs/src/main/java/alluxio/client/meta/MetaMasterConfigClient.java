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

package alluxio.client.meta;

import alluxio.conf.PropertyKey;
import alluxio.wire.Configuration;

import java.io.Closeable;
import java.io.IOException;

/**
 * Interface for a meta master config client.
 */
public interface MetaMasterConfigClient extends Closeable {
  /**
   * @return the runtime configuration
   */
  Configuration getConfiguration() throws IOException;

  /**
   * Sets a property for a path.
   *
   * @param path the path
   * @param key the property key
   * @param value the property value
   */
  void setPathConfiguration(String path, PropertyKey key, String value) throws IOException;
}
