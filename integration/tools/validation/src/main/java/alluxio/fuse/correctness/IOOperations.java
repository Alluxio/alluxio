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

package alluxio.fuse.correctness;

/**
 * The valid operations that can be tested by the Fuse correctness validation tool.
 */
public enum IOOperations {
  READ,
  WRITE,
  ;

  /**
   * Converts operation string to {@link IOOperations}.
   *
   * @param operationStr the operation in string format
   * @return the operation
   */
  public static IOOperations fromString(String operationStr) {
    for (IOOperations type : IOOperations.values()) {
      if (type.toString().equalsIgnoreCase(operationStr)) {
        return type;
      }
    }
    throw new IllegalArgumentException(String.format("Operation %s is not valid.", operationStr));
  }
}
