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
 * The method option for deleting a lineage.
 */
@PublicApi
public final class DeleteLineageOptions {
  /** Whether to delete all downstream lineages */
  private boolean mCascade;

  /**
   * @return the default options
   */
  public static DeleteLineageOptions defaults() {
    return new DeleteLineageOptions();
  }

  private DeleteLineageOptions() {
    mCascade = false;
  }

  /**
   * @return the cascade flag value; if the delete is cascade, it will delete all the downstream
   *         lineages that depend on the given one recursively
   */
  public boolean isCascade() {
    return mCascade;
  }

  /**
   * Sets the cascade flag for this option.
   *
   * @return the updated options object
   */
  public DeleteLineageOptions setCascade(boolean cascade) {
    mCascade = cascade;
    return this;
  }
}
