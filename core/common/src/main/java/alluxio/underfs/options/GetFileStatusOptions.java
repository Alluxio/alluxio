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

package alluxio.underfs.options;

/**
 * Method options for getting the status of a file in {@link alluxio.underfs.UnderFileSystem}.
 */
public class GetFileStatusOptions {
  private boolean mIncludeRealContentHash = false;

  /**
   * @return whether include real content hash
   */
  public boolean isIncludeRealContentHash() {
    return mIncludeRealContentHash;
  }

  /**
   * @param includeRealContentHash include real content hash flag value
   * @return the updated options object
   */
  public GetFileStatusOptions setIncludeRealContentHash(boolean includeRealContentHash) {
    mIncludeRealContentHash = includeRealContentHash;
    return this;
  }

  /**
   * @return the default {@link GetFileStatusOptions}
   */
  public static GetFileStatusOptions defaults() {
    return new GetFileStatusOptions();
  }
}
