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

package alluxio.underfs;

import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Information about a directory in the under file system.
 */
@NotThreadSafe
public class UfsDirectoryStatus extends UfsStatus {

  /**
   * Creates new instance of {@link UfsDirectoryStatus}.
   *
   * @param name relative path of directory
   * @param owner of the directory
   * @param group of the directory
   * @param mode of the directory
   * @param lastModifiedTimeMs of the directory
   * @param xAttr extended attributes, if any
   */
  public UfsDirectoryStatus(String name, String owner, String group, short mode,
      Long lastModifiedTimeMs, @Nullable Map<String, byte[]> xAttr) {
    super(name, true, owner, group, mode, lastModifiedTimeMs, xAttr);
  }

  /**
   * Creates new instance of {@link UfsDirectoryStatus}.
   *
   * @param name relative path of directory
   * @param owner of the directory
   * @param group of the directory
   * @param mode of the directory
   * @param lastModifiedTimeMs of the directory
   */
  public UfsDirectoryStatus(String name, String owner, String group, short mode,
      Long lastModifiedTimeMs) {
    super(name, true, owner, group, mode, lastModifiedTimeMs, null);
  }

  /**
   * Creates new instance of {@link UfsDirectoryStatus} without providing last modified time or
   * extended attributes.
   *
   * @param name relative path of directory
   * @param owner of the directory
   * @param group of the directory
   * @param mode of the directory
   */
  public UfsDirectoryStatus(String name, String owner, String group, short mode) {
    super(name, true, owner, group, mode, null, null);
  }

  /**
   * Creates a new instance of under directory information as a copy.
   *
   * @param status directory information to copy
   */
  public UfsDirectoryStatus(UfsDirectoryStatus status) {
    super(status);
  }

  @Override
  public UfsDirectoryStatus copy() {
    return new UfsDirectoryStatus(this);
  }

  @Override
  public String toString() {
    return toStringHelper().toString();
  }
}
