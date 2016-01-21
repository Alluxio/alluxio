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

/**
 * Method option for freeing space.
 */
@PublicApi
public final class FreeOptions {
  private boolean mRecursive;

  /**
   * @return the default {@link FreeOptions}
   */
  public static FreeOptions defaults() {
    return new FreeOptions();
  }

  private FreeOptions() {
    mRecursive = false;
  }

  /**
   * @return the recursive flag value; if the object to be freed is a directory, the flag specifies
   *         whether the directory content should be recursively freed as well
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * Sets the recursive flag.
   *
   * @param recursive the recursive flag value to use; if the object to be freed is a directory,
   *        the flag specifies whether the directory content should be recursively freed as well
   * @return the updated options object
   */
  public FreeOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
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
