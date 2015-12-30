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
 * Method option for freeing space.
 */
@PublicApi
public final class FreeOptions {

  /**
   * Builder for {@link FreeOptions}.
   */
  public static class Builder implements OptionsBuilder<FreeOptions> {
    private boolean mRecursive;

    /**
     * Creates a new builder for {@link FreeOptions}.
     */
    public Builder() {
      this(ClientContext.getConf());
    }

    /**
     * Creates a new builder for {@link FreeOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mRecursive = false;
    }

    /**
     * Sets the recursive flag.
     *
     * @param recursive the recursive flag value to use; if the object to be freed is a directory,
     *        the flag specifies whether the directory content should be recursively freed as well
     * @return the builder
     */
    public Builder setRecursive(boolean recursive) {
      mRecursive = recursive;
      return this;
    }

    /**
     * Builds a new instance of {@link FreeOptions}.
     *
     * @return a {@link FreeOptions} instance
     */
    @Override
    public FreeOptions build() {
      return new FreeOptions(this);
    }
  }

  /**
   * @return the default {@link FreeOptions}
   */
  public static FreeOptions defaults() {
    return new Builder().build();
  }

  private final boolean mRecursive;

  private FreeOptions(FreeOptions.Builder builder) {
    mRecursive = builder.mRecursive;
  }

  /**
   * @return the recursive flag value; if the object to be freed is a directory, the flag specifies
   *         whether the directory content should be recursively freed as well
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("FreeOptions(");
    sb.append(super.toString()).append(", Recursive: ").append(mRecursive);
    sb.append(")");
    return sb.toString();
  }
}
