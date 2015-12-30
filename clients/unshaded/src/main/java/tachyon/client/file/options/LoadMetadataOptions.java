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

/**
 * Method option for loading the metadata.
 */
@PublicApi
public final class LoadMetadataOptions {

  /**
   * Builder for {@link LoadMetadataOptions}.
   */
  public static class Builder implements OptionsBuilder<LoadMetadataOptions> {
    private boolean mRecursive;

    /**
     * Creates a new builder for {@link LoadMetadataOptions}.
     */
    public Builder() {
      this(ClientContext.getConf());
    }

    /**
     * Creates a new builder for {@link LoadMetadataOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mRecursive = false;
    }

    /**
     * Sets the recursive flag.
     *
     * @param recursive the recursive flag value to use; it specifies whether parent directories
     *        should be created if they do not already exist
     * @return the builder
     */
    public Builder setRecursive(boolean recursive) {
      mRecursive = recursive;
      return this;
    }

    /**
     * Builds a new instance of {@link LoadMetadataOptions}.
     *
     * @return a {@link LoadMetadataOptions} instance
     */
    @Override
    public LoadMetadataOptions build() {
      return new LoadMetadataOptions(this);
    }
  }

  /**
   * @return the default {@link LoadMetadataOptions}
   */
  public static LoadMetadataOptions defaults() {
    return new Builder().build();
  }

  private final boolean mRecursive;

  private LoadMetadataOptions(LoadMetadataOptions.Builder builder) {
    mRecursive = builder.mRecursive;
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("LoadMetadataOptions(");
    sb.append(super.toString()).append(", Recursive: ").append(mRecursive);
    sb.append(")");
    return sb.toString();
  }
}
