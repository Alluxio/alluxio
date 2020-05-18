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

package alluxio.stress.client;

/**
 * The operations for the client io stress tests.
 */
public enum ClientIOOperation {
  Write,  // write the files for the read operations

  ReadArray,       // random reads must call seek()
  ReadByteBuffer,  // random reads must call seek()
  ReadFully,       // random reads must call seek()
  PosRead,         // random reads uses random offsets to api
  PosReadFully,    // random reads uses random offsets to api
  ;

  /**
   * @param operation the operation
   * @return true if the operation is a read
   */
  public static boolean isRead(ClientIOOperation operation) {
    switch (operation) {
      case ReadArray:
      case ReadByteBuffer:
      case ReadFully:
      case PosRead:
      case PosReadFully:
        return true;
      default:
        return false;
    }
  }

  /**
   * @param operation the operation
   * @return true if the operation is a positioned read
   */
  public static boolean isPosRead(ClientIOOperation operation) {
    switch (operation) {
      case PosRead:
      case PosReadFully:
        return true;
      default:
        return false;
    }
  }
}
