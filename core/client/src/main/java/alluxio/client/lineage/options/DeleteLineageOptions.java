/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.lineage.options;

import alluxio.annotation.PublicApi;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The method option for deleting a lineage.
 */
@PublicApi
@NotThreadSafe
public final class DeleteLineageOptions {
  /** Whether to delete all downstream lineages. */
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
   * @param cascade true if the deletion is cascading, false otherwise
   * @return the updated options object
   */
  public DeleteLineageOptions setCascade(boolean cascade) {
    mCascade = cascade;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DeleteLineageOptions)) {
      return false;
    }
    DeleteLineageOptions that = (DeleteLineageOptions) o;
    return Objects.equal(mCascade, that.mCascade);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCascade);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("cascade", mCascade)
        .toString();
  }
}
