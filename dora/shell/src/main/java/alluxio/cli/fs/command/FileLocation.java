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

package alluxio.cli.fs.command;

import java.util.Objects;
import java.util.Set;

/**
 * Description of the file location.
 */
public class FileLocation {
  private final String mFileName;

  private final String mPreferredWorker;

  private final boolean mDataOnPreferredWorker;

  private final Set<String> mWorkersThatHaveData;

  /**
   * Description of the file location.
   * @param fileName the file name
   * @param preferredWorker the preferred worker
   * @param dataOnPreferredWorker the
   * @param workers the workers where the file locate
   */
  public FileLocation(String fileName, String preferredWorker, boolean dataOnPreferredWorker,
                      Set<String> workers) {
    mFileName = fileName;
    mPreferredWorker = preferredWorker;
    mDataOnPreferredWorker = dataOnPreferredWorker;
    mWorkersThatHaveData = workers;
  }

  /**
   * Get the file name.
   * @return the file name
   */
  public String getFileName() {
    return mFileName;
  }

  /**
   * Get the preferred worker.
   * @return the preferred worker
   */
  public String getPreferredWorker() {
    return mPreferredWorker;
  }

  /**
   * If data locates at the preferred worker.
   * @return if data locates at the preferred worker
   */
  public boolean isDataOnPreferredWorker() {
    return mDataOnPreferredWorker;
  }

  /**
   * Get the workers where data locate.
   * @return the workers set where data locate
   */
  public Set<String> getWorkersThatHaveData() {
    return mWorkersThatHaveData;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileLocation that = (FileLocation) o;
    return mDataOnPreferredWorker == that.mDataOnPreferredWorker
        && Objects.equals(mFileName, that.mFileName)
        && Objects.equals(mPreferredWorker, that.mPreferredWorker)
        && Objects.equals(mWorkersThatHaveData, that.mWorkersThatHaveData);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mFileName, mPreferredWorker, mDataOnPreferredWorker, mWorkersThatHaveData);
  }
}
