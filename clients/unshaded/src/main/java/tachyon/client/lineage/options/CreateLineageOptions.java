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

/**
 * Method option for creating lineage.
 */
@PublicApi
public final class CreateLineageOptions {
  /**
   * @return the default options
   */
  public static CreateLineageOptions defaults() {
    return new CreateLineageOptions();
  }

  /** Flag indicating whether or not to overwrite, currently unsupported */
  private boolean mOverwrite;

  private CreateLineageOptions() {
    mOverwrite = false;
  }

  /**
   * @return whether the overwrite flag is set
   */
  public boolean isOverwrite() {
    return mOverwrite;
  }

  /**
   * @param overwrite the overwrite flag to set
   * @return the updated options object
   */
  public CreateLineageOptions setOverwrite(boolean overwrite) {
    mOverwrite = overwrite;
    return this;
  }
}
