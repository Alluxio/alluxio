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

public class FileLocation {
  private final String fileName;

  private final String preferredWorker;

  private final boolean dataOnPreferredWorker;

  private final Set<String> workersThatHaveData;

  public FileLocation(String fileName, String preferredWorker, boolean dataOnPreferredWorker,
                      Set<String> workers) {
    this.fileName = fileName;
    this.preferredWorker = preferredWorker;
    this.dataOnPreferredWorker = dataOnPreferredWorker;
    this.workersThatHaveData = workers;
  }

  public String getFileName() {
    return fileName;
  }

  public String getPreferredWorker() {
    return preferredWorker;
  }

  public boolean isDataOnPreferredWorker() {
    return dataOnPreferredWorker;
  }

  public Set<String> getWorkersThatHaveData() {
    return workersThatHaveData;
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
    return dataOnPreferredWorker == that.dataOnPreferredWorker &&
        Objects.equals(fileName, that.fileName) &&
        Objects.equals(preferredWorker, that.preferredWorker) &&
        Objects.equals(workersThatHaveData, that.workersThatHaveData);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileName, preferredWorker, dataOnPreferredWorker, workersThatHaveData);
  }
}