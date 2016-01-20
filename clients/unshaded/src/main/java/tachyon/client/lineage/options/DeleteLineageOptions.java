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
 * The method option for deleting a lineage.
 */
@PublicApi
public final class DeleteLineageOptions {

  /**
   * Builder for the {@link DeleteLineageOptions} class.
   */
  public static class Builder {
    private boolean mCascade;

    /**
     * Creates a new builder for {@link DeleteLineageOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mCascade = false;
    }

    /**
     * @param cascade the cascade flag to use; if the delete is cascade, it will delete all the
     *        downstream lineages that depend on the given one recursively.
     *
     * @return the builder
     */
    public Builder setCascade(boolean cascade) {
      mCascade = cascade;
      return this;
    }

    /**
     * @return builds a new instance of {@link DeleteLineageOptions}
     */
    public DeleteLineageOptions build() {
      return new DeleteLineageOptions(this);
    }
  }

  /**
   * @return the default options
   */
  public static DeleteLineageOptions defaults() {
    return new Builder(ClientContext.getConf()).build();
  }

  private final boolean mCascade;

  private DeleteLineageOptions(DeleteLineageOptions.Builder builder) {
    mCascade = builder.mCascade;
  }

  /**
   * @return the cascade flag value; if the delete is cascade, it will delete all the downstream
   *         lineages that depend on the given one recursively
   */
  public boolean isCascade() {
    return mCascade;
  }
}
