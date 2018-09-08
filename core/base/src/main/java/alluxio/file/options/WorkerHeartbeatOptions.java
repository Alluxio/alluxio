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

package alluxio.file.options;

import com.google.common.base.Objects;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for the worker to master heartbeat.
 *
 * @param <T> the type of the concrete subclass
 */
@NotThreadSafe
public abstract class WorkerHeartbeatOptions<T extends WorkerHeartbeatOptions> {
  protected List<String> mPersistedUfsFingerprintList;

  /**
   * @return list of ufs fingerprints, of the persisted files
   */
  public List<String> getPersistedUfsFingerprintList() {
    return mPersistedUfsFingerprintList;
  }

  /**
   * @param persistedUfsFingerprintList the list of ufs fingerprints, of persisted files
   * @return the list of persisted fingerprints
   */
  public T setPersistedUfsFingerprintList(List<String> persistedUfsFingerprintList) {
    mPersistedUfsFingerprintList = persistedUfsFingerprintList;
    return (T) this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WorkerHeartbeatOptions)) {
      return false;
    }
    WorkerHeartbeatOptions that = (WorkerHeartbeatOptions) o;
    return Objects.equal(mPersistedUfsFingerprintList, that.mPersistedUfsFingerprintList);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPersistedUfsFingerprintList);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("persistedUfsFingerprintList", mPersistedUfsFingerprintList)
        .toString();
  }
}
