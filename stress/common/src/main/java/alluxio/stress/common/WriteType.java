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
 * Write type, MUST_CACHE, CACHE_THROUGH, THROUGH, ASYNC_THROUGH, ALL or NOT_SET.
 */
public enum WriteType {
  /**
   * Write the file, guaranteeing the data is written to Alluxio storage or failing the operation.
   * The data will be written to the highest tier in a worker's storage. Data will not be
   * persisted to the under storage.
   */
  MUST_CACHE("MUST_CACHE"),
  /**
   * Write the file synchronously to the under fs, and also try to write to the highest tier in a
   * worker's Alluxio storage.
   */
  CACHE_THROUGH("CACHE_THROUGH"),
  /**
   * Write the file synchronously to the under fs, skipping Alluxio storage.
   */
  THROUGH("THROUGH"),
  /**
   * Write the file asynchronously to the under fs.
   */
  ASYNC_THROUGH("ASYNC_THROUGH"),

  /**
   * Write the file in all possible way.
   */
  ALL("ALL"),

  /**
   * Do not set this property.
   */
  NOT_SET("NOT_SET")
  ;

  private final String mName;

  WriteType(String name) {
    mName = name;
  }

  /**
   * @return whether write type is ALL
   */
  public boolean isAll() {
    if (this == WriteType.ALL)
    {
      return true;
    }
    return false;
  }

  /**
   * @return whether write type is ALL
   */
  public boolean isNotSet() {
    if (this == WriteType.NOT_SET)
    {
      return true;
    }
    return false;
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
  public static WriteType fromString(String text) {
    for (WriteType type : WriteType.values()) {
      if (type.toString().equalsIgnoreCase(text)) {
        return type;
      }
    }
    throw new IllegalArgumentException("No constant with text " + text + " found");
  }
}
