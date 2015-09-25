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
import tachyon.client.ClientContext;
import tachyon.conf.TachyonConf;

@PublicApi
public final class ListStatusOptions {
  public static class Builder {
    /**
     * Creates a new builder for {@link ListStatusOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {}

    /**
     * Builds a new instance of {@code ListStatusOptions}.
     *
     * @return a {@code ListStatusOptions} instance
     */
    public ListStatusOptions build() {
      return new ListStatusOptions(this);
    }
  }

  /**
   * @return the default {@code ListStatusOptions}
   */
  public static ListStatusOptions defaults() {
    return new Builder(ClientContext.getConf()).build();
  }

  private ListStatusOptions(ListStatusOptions.Builder builder) {}
}
