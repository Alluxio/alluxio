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
 * Method option for deleting a file.
 */
@PublicApi
public final class DeleteOptions {

  /**
   * Builder for {@link DeleteOptions}.
   */
  public static class Builder implements OptionsBuilder<DeleteOptions> {
    private boolean mRecursive;

    /**
     * Creates a new builder for {@link DeleteOptions}.
     */
    public Builder() {
      this(ClientContext.getConf());
    }

    /**
     * Creates a new builder for {@link DeleteOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mRecursive = false;
    }

    /**
     * Sets the recursive flag.
     *
     * @param recursive the recursive flag value to use; if the object to be deleted is a directory,
     *        the flag specifies whether the directory content should be recursively deleted as well
     * @return the builder
     */
    public Builder setRecursive(boolean recursive) {
      mRecursive = recursive;
      return this;
    }

    /**
     * Builds a new instance of {@link DeleteOptions}.
     *
     * @return a {@link DeleteOptions} instance
     */
    @Override
    public DeleteOptions build() {
      return new DeleteOptions(this);
    }
  }

  /**
   * @return the default {@link DeleteOptions}
   */
  public static DeleteOptions defaults() {
    return new Builder().build();
  }

  private final boolean mRecursive;

  private DeleteOptions(DeleteOptions.Builder builder) {
    mRecursive = builder.mRecursive;
  }

  /**
   * @return the recursive flag value; if the object to be deleted is a directory, the flag
   *         specifies whether the directory content should be recursively deleted as well
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("DeleteOptions(");
    sb.append(super.toString()).append(", Recursive: ").append(mRecursive);
    sb.append(")");
    return sb.toString();
  }
}
