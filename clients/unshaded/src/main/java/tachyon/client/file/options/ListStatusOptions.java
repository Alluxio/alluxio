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
 * Method option for listing the status.
 */
@PublicApi
public final class ListStatusOptions {

  /**
   * Builder for {@link ListStatusOptions}.
   */
  public static class Builder implements OptionsBuilder<ListStatusOptions> {
    /**
     * Creates a new builder for {@link ListStatusOptions}.
     */
    public Builder() {}

    /**
     * Creates a new builder for {@link ListStatusOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {}

    /**
     * Builds a new instance of {@link ListStatusOptions}.
     *
     * @return a {@link ListStatusOptions} instance
     */
    @Override
    public ListStatusOptions build() {
      return new ListStatusOptions(this);
    }
  }

  /**
   * @return the default {@link ListStatusOptions}
   */
  public static ListStatusOptions defaults() {
    return new Builder().build();
  }

  private ListStatusOptions(ListStatusOptions.Builder builder) {}
}
