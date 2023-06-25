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

/**
 * A POJO for providing File Information HTTP Response.
 */
public class ResponseFileInfo {

  private final String mType;

  private final String mName;

  /**
   * A POJO for providing File Information HTTP Response.
   * @param type the file type (file or directory)
   * @param name the file name
   */
  public ResponseFileInfo(String type, String name) {
    mType = type;
    mName = name;
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
}
