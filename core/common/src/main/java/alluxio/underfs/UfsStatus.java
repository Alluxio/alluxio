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

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Information about a file or a directory in the under file system. Listing contents in a
 * {@link UnderFileSystem} returns entries of this class.
 */
@NotThreadSafe
public abstract class UfsStatus {
  protected final boolean mIsDirectory;
  protected String mName;

  // Permissions
  protected final String mOwner;
  protected final String mGroup;
  protected final short mMode;

  /**
   * Creates new instance of {@link UfsStatus}.
   *
   * @param name relative path of file or directory
   * @param isDirectory whether the path is a directory
   * @param owner of the file
   * @param group of the file
   * @param mode of the file
   */
  protected UfsStatus(String name, boolean isDirectory, String owner, String group, short mode) {
    mIsDirectory = isDirectory;
    mName = name;
    mOwner = owner;
    mGroup = group;
    mMode = mode;
  }

  /**
   * Creates a new instance of {@link UfsStatus} as a copy.
   *
   * @param status file information to copy
   */
  protected UfsStatus(UfsStatus status) {
    mIsDirectory = status.mIsDirectory;
    mName = status.mName;
    mOwner = status.mOwner;
    mGroup = status.mGroup;
    mMode = status.mMode;
  }

  /**
   * Create a copy of {@link UfsStatus}.
   *
   * @return new instance as a copy
   */
  public abstract UfsStatus copy();

  /**
   * Converts an array of UFS file status to a listing result where each element in the array is
   * a file or directory name.
   *
   * @param children array of listing statuses
   * @return array of file or directory names, or null if the input is null
   */
  public static String[] convertToNames(UfsStatus[] children) {
    if (children == null) {
      return null;
    }
    String[] ret = new String[children.length];
    for (int i = 0; i < children.length; ++i) {
      ret[i] = children[i].getName();
    }
    return ret;
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
   * Gets the group of the given path.
   *
   * @return the group of the file
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * Gets the mode of the given path in short format, e.g 0700.
   *
   * @return the mode of the file
   */
  public short getMode() {
    return mMode;
  }

  /**
   * @return name of file or directory
   */
  public String getName() {
    return mName;
  }

  /**
   * Gets the owner of the given path.
   *
   * @return the owner of the path
   */
  public String getOwner() {
    return mOwner;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mName, mIsDirectory, mOwner, mGroup, mMode);
  }

  /**
   * Set the name of file or directory.
   *
   * @param name of entry
   * @return this object
   */
  public UfsStatus setName(String name) {
    mName = name;
    return this;
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UfsStatus)) {
      return false;
    }
    UfsStatus that = (UfsStatus) o;
    return Objects.equal(mName, that.mName)
        && Objects.equal(mIsDirectory, that.mIsDirectory)
        && Objects.equal(mOwner, that.mOwner)
        && Objects.equal(mGroup, that.mGroup)
        && Objects.equal(mMode, that.mMode);
  }
}
