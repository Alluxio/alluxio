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

package alluxio.wire;

import com.google.common.base.Objects;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The startup consistency check information.
 */
@NotThreadSafe
public class StartupConsistencyCheck {
  private List<String> mInconsistentUris;
  private String mStatus;

  /**
   * Creates a new instance of {@link StartupConsistencyCheck}.
   */
  public StartupConsistencyCheck() {}

  /**
   * @return the inconsistent URIs
   */
  public List<String> getInconsistentUris() {
    return mInconsistentUris;
  }

  /**
   * @return the status
   */
  public String getStatus() {
    return mStatus;
  }

  /**
   * @param uris the inconsistent URIs
   * @return the consistency check's result
   */
  public StartupConsistencyCheck setInconsistentUris(List<String> uris) {
    mInconsistentUris = uris;
    return this;
  }

  /**
   * @param status the status of the consistency check
   * @return the consistency check's result
   */
  public StartupConsistencyCheck setStatus(String status) {
    mStatus = status;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StartupConsistencyCheck)) {
      return false;
    }
    StartupConsistencyCheck that = (StartupConsistencyCheck) o;
    return Objects.equal(mInconsistentUris, that.mInconsistentUris)
        && Objects.equal(mStatus, that.mStatus);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mInconsistentUris, mStatus);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("inconsistent URIs", mInconsistentUris)
        .add("status", mStatus).toString();
  }
}
