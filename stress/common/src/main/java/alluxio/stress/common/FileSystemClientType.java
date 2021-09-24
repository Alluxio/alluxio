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

package alluxio.stress.common;

/**
 * FileSystemClientType, Alluxio Native API or Alluxio HDFS API.
 */
public enum FileSystemClientType {

  /** Alluxio file system API. */
  ALLUXIO_NATIVE("AlluxioNative"),

  /** Alluxio Hadoop Compatible File System API. */
  ALLUXIO_HDFS("AlluxioHDFS"),
  ;

  private final String mName;

  /**
   * Constructor.
   *
   * @param name of the client type
   */
  FileSystemClientType(String name) {
    mName = name;
  }

  @Override
  public String toString() {
    return mName;
  }

  /**
   * Creates an instance type from the string. This method is case insensitive.
   *
   * @param text the instance type in string
   * @return the created instance
   */
  public static FileSystemClientType fromString(String text) {
    for (FileSystemClientType type : FileSystemClientType.values()) {
      if (type.toString().equalsIgnoreCase(text)) {
        return type;
      }
    }
    throw new IllegalArgumentException("No constant with text " + text + " found");
  }
}
