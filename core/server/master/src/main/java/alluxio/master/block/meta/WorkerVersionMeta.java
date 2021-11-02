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

package alluxio.master.block.meta;

/**
 * An object representation of the worker metadata.This class is not thread safe
 * so accessing or updating the fields need to use AtomicReference<WorkerTimeMeta>.
 */
public class WorkerVersionMeta {
  /** The version of worker. */
  private String mVersion;
  /** The revision of worker. */
  private String mRevision;

  /**
   * Constructor.
   *
   */
  public WorkerVersionMeta(String version, String revision) {
    mVersion = version;
    mRevision = revision;
  }

  /**
   * Gets the version of worker.
   * @return the version of worker
   */
  public String getVersion() {
    return mVersion;
  }

  /**
   * Gets the revision of worker.
   * @return the revision of worker
   */
  public String getRevision() {
    return mRevision;
  }
}
