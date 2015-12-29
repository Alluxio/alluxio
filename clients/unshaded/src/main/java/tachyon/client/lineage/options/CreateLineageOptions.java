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

package tachyon.client.lineage.options;

import tachyon.annotation.PublicApi;
import tachyon.client.ClientContext;
import tachyon.conf.TachyonConf;

/**
 * Method option for creating lineage.
 */
@PublicApi
public final class CreateLineageOptions {
  private static class Builder {
    /**
     * Creates a new builder for {@link CreateLineageOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {}

    /**
     * @return builds a new instance of {@link CreateLineageOptions}
     */
    public CreateLineageOptions build() {
      return new CreateLineageOptions(this);
    }
  }

  /**
   * @return the default options
   */
  public static CreateLineageOptions defaults() {
    return new Builder(ClientContext.getConf()).build();
  }

  private CreateLineageOptions(CreateLineageOptions.Builder buidler) {}
}
