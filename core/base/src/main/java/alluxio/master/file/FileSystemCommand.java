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

package alluxio.master.file;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import alluxio.AlluxioURI;

/**
 * Class to represent the status and result of the startup consistency check.
 */
public final class FileSystemCommand {
  /**
   * Status of the check.
   */
  public enum Status {
    COMPLETE,
    DISABLED,
    FAILED,
    NOT_STARTED,
    RUNNING
  }

  /**
   * @param inconsistentUris the uris which are inconsistent with the underlying storage
   * @return a result set to the complete status
   */
  public static FileSystemCommand complete(List<AlluxioURI> inconsistentUris) {
    return new FileSystemCommand(Status.COMPLETE, inconsistentUris);
  }

  /**
   * @return a result set to the disabled status
   */
  public static FileSystemCommand disabled() {
    return new FileSystemCommand(Status.DISABLED, new ArrayList<AlluxioURI>());
  }

  /**
   * @return a result set to the disabled status
   */
  public static FileSystemCommand notStarted() {
    return new FileSystemCommand(Status.NOT_STARTED, new ArrayList<AlluxioURI>());
  }

  /**
   * @return a result set to the failed status
   */
  public static FileSystemCommand failed() {
    return new FileSystemCommand(Status.FAILED, new ArrayList<AlluxioURI>());
  }

  /**
   * @return a result set to the running status
   */
  public static FileSystemCommand running() {
    return new FileSystemCommand(Status.RUNNING, new ArrayList<AlluxioURI>());
  }

  private final Status mStatus;
  private final List<AlluxioURI> mInconsistentUris;

  /**
   * Create a new startup consistency check result.
   *
   * @param status the state of the check
   * @param inconsistentUris the uris which are inconsistent with the underlying storage
   */
  private FileSystemCommand(Status status, List<AlluxioURI> inconsistentUris) {
    mStatus = Preconditions.checkNotNull(status, "status");
    mInconsistentUris = Preconditions.checkNotNull(inconsistentUris, "inconsistentUris");
  }

  /**
   * @return the status of the check
   */
  public Status getStatus() {
    return mStatus;
  }

  /**
   * @return the uris which are inconsistent with the underlying storage
   */
  public List<AlluxioURI> getInconsistentUris() {
    return mInconsistentUris;
  }
}
