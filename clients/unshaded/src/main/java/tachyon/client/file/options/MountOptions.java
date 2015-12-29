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

package tachyon.client.file.options;

import tachyon.annotation.PublicApi;
import tachyon.conf.TachyonConf;

/**
 * Method option for mounting a path.
 */
@PublicApi
public final class MountOptions {

  /**
   * Builder for {@link MountOptions}.
   */
  public static class Builder implements OptionsBuilder<MountOptions> {
    /**
     * Creates a new builder for {@link MountOptions}.
     */
    public Builder() {}

    /**
     * Creates a new builder for {@link MountOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {}

    /**
     * Builds a new instance of {@link MountOptions}.
     *
     * @return a {@link MountOptions} instance
     */
    @Override
    public MountOptions build() {
      return new MountOptions(this);
    }
  }

  /**
   * @return the default {@link MountOptions}
   */
  public static MountOptions defaults() {
    return new Builder().build();
  }

  private MountOptions(MountOptions.Builder builder) {}
}
