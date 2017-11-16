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

package alluxio.master.file.options;

import alluxio.thrift.FileSystemHeartbeatTOptions;
import alluxio.thrift.UfsFileTStatus;
import alluxio.underfs.UfsFileStatus;

import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for syncing the metadata of a path.
 */
@NotThreadSafe
public final class WorkerHeartbeatOptions {
  private List<UfsFileStatus> mPersistedFileStatusList;

  /**
   * @return the default {@link WorkerHeartbeatOptions}
   */
  public static WorkerHeartbeatOptions defaults() {
    return new WorkerHeartbeatOptions();
  }

  /**
   * Constructs an instance of {@link WorkerHeartbeatOptions} from
   * {@link FileSystemHeartbeatTOptions}.
   *
   * @param options the {@link FileSystemHeartbeatTOptions} to use
   */
  public WorkerHeartbeatOptions(FileSystemHeartbeatTOptions options) {
    this();
    if (options != null && options.isSetPersistedFileStatuses()) {
      mPersistedFileStatusList.clear();
      for (UfsFileTStatus tStatus : options.getPersistedFileStatuses()) {
        mPersistedFileStatusList.add(new UfsFileStatus(tStatus));
      }
    }
  }

  private WorkerHeartbeatOptions() {
    mPersistedFileStatusList = new ArrayList<>();
  }

  /**
   * @return list of file status, of the persisted files
   */
  public List<UfsFileStatus> getPersistedFileStatusList() {
    return mPersistedFileStatusList;
  }

  /**
   * @param persistedFileStatusList the list of file status, of persisted files
   */
  public void setPersistedFileStatusList(List<UfsFileStatus> persistedFileStatusList) {
    mPersistedFileStatusList = persistedFileStatusList;
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
    return Objects.equal(mPersistedFileStatusList, that.mPersistedFileStatusList);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPersistedFileStatusList);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("persistedFileStatusList", mPersistedFileStatusList)
        .toString();
  }
}
