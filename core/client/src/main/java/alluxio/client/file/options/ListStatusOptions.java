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

package alluxio.client.file.options;

import alluxio.annotation.PublicApi;
import alluxio.thrift.ListStatusTOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for listing the status.
 */
@PublicApi
@NotThreadSafe
public final class ListStatusOptions {
  private boolean mLoadDirectChildren;

  /**
   * @return the default {@link ListStatusOptions}
   */
  public static ListStatusOptions defaults() {
    return new ListStatusOptions();
  }

  private ListStatusOptions() {
    mLoadDirectChildren = true;
  }

  /**
   * @return the load direct children flag. It specifies whether the direct children should
   *         be loaded from UFS if they have not been loaded once.
   */
  public boolean isLoadDirectChildren() {
    return mLoadDirectChildren;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ListStatusOptions)) {
      return false;
    }
    ListStatusOptions that = (ListStatusOptions) o;
    return Objects.equal(mLoadDirectChildren, that.mLoadDirectChildren);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLoadDirectChildren);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("loadDirectChildren", mLoadDirectChildren)
        .toString();
  }

  /**
   * @return thrift representation of the options
   */
  public ListStatusTOptions toThrift() {
    ListStatusTOptions options = new ListStatusTOptions();
    options.setLoadDirectChildren(mLoadDirectChildren);
    return options;
  }
}
