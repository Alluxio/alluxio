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

import java.util.List;

/**
 * Class to represent a persist options.
 */
public final class PersistCommandOptions {
  private List<PersistFile> mFilesToPersist;

  /**
   * Creates a new instance of {@link PersistCommandOptions}.
   *
   * @param filesToPersist the list of files to persist
   */
  public PersistCommandOptions(List<PersistFile> filesToPersist) {
    mFilesToPersist = filesToPersist;
  }

  /**
   * @return the list of files to persist
   */
  public List<PersistFile> getFilesToPersist() {
    return mFilesToPersist;
  }

  /**
   * Set the list of files to persist.
   *
   * @param filesToPersist the list of files to persist
   */
  public void setFilesToPersist(List<PersistFile> filesToPersist) {
    mFilesToPersist = filesToPersist;
  }
}
