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

package alluxio.worker.http;

import alluxio.util.FormatUtils;

/**
 * A POJO for providing File Information HTTP Response.
 */
public class ResponseFileInfo {

  private final String mType;

  private final String mName;

  private final String mPath;

  private final String mUfsPath;

  private final long mLastModificationTimeMs;

  private final long mLength;

  private final String mHumanReadableFileSize;

  /**
   * A POJO for providing File Information HTTP Response.
   * @param type the file type (file or directory)
   * @param name the file name
   * @param path the Alluxio path of the file
   * @param ufsPath the UFS path of the file
   * @param lastModificationTimeMs the last modification timestamp (milliseconds) of the file
   * @param length the length of the file
   */
  public ResponseFileInfo(String type, String name, String path, String ufsPath,
                          long lastModificationTimeMs, long length) {
    mType = type;
    mName = name;
    mPath = path;
    mUfsPath = ufsPath;
    mLastModificationTimeMs = lastModificationTimeMs;
    mLength = length;
    mHumanReadableFileSize = FormatUtils.getSizeFromBytes(length);
  }

  /**
   * Get the file type.
   * @return either directory or file
   */
  public String getType() {
    return mType;
  }

  /**
   * Get the name of the file.
   * @return the file name
   */
  public String getName() {
    return mName;
  }

  /**
   * Get the Alluxio path of the file.
   * @return the Alluxio path of the file
   */
  public String getPath() {
    return mPath;
  }

  /**
   * Get the UFS path of the file.
   * @return the UFS path of the file
   */
  public String getUfsPath() {
    return mUfsPath;
  }

  /**
   * Get the last modification timestamp (milliseconds) of the file.
   * @return the last modification timestamp (milliseconds) of the file
   */
  public long getLastModificationTimeMs() {
    return mLastModificationTimeMs;
  }

  /**
   * Get the length of the file.
   * @return the length of the file
   */
  public long getLength() {
    return mLength;
  }

  /**
   * Get the formatted size.
   * @return the formatted size
   */
  public String getHumanReadableFileSize() {
    return mHumanReadableFileSize;
  }
}
