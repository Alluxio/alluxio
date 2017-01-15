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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Information about a file or a directory in the under file system. Listing contents in a
 * {@link UnderFileSystem} returns entries of this class.
 */
@NotThreadSafe
public class UnderFileStatus {
  private final boolean mIsDirectory;
  private final String mName;

  /**
   * Creates new instance for under file information.
   *
   * @param name relative path of file or directory
   * @param isDirectory whether the path is a directory
   */
  public UnderFileStatus(String name, boolean isDirectory) {
    mIsDirectory = isDirectory;
    mName = name;
  }

  /**
   * @return true, if the path is a directory
   */
  public boolean isDirectory() {
    return mIsDirectory;
  }

  /**
   * @return true, if the path is a file
   */
  public boolean isFile() {
    return !mIsDirectory;
  }

  /**
   * @return name of file or directory
   */
  public String getName() {
    return mName;
  }

  @Override
  public String toString() {
    return getName();
  }

  /**
   * Converts an array of UFS file status to a listing result where each element in the array is
   * a file or directory name.
   *
   * @param children array of listing statuses
   * @return array of file or directory names, or null if the input is null
   */
  public static String[] convertToNames(UnderFileStatus[] children) {
    if (children == null) {
      return null;
    }
    String[] ret = new String[children.length];
    for (int i = 0; i < children.length; ++i) {
      ret[i] = children[i].getName();
    }
    return ret;
  }
}
