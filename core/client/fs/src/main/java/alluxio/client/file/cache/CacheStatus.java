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

package alluxio.client.file.cache;

import java.util.Optional;

/**
 * Mixin interface for various cache status info.
 */
public interface CacheStatus {
  /**
   * Gets cache usage.
   *
   * @return cache usage, or none if reporting usage info is not supported
   */
  Optional<CacheUsage> getUsage();
}
