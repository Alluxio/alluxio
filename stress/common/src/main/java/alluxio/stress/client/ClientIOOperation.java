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
 *
 * There are 2 main types of reads: streaming and positioned.
 * The streaming reads do not take file offsets as part of the api, so a random, streaming read
 * is implemented with a seek() call before the read call. For positioned reads, the file offset
 * is an explicit parameter in the api, so no seek() calls are necessary to read from any file
 * offset.
 */
public enum ClientIOOperation {
  /** The write operation, for writing data for the read operations. */
  Write,

  /** Streaming read api, using arrays. */
  ReadArray,
  /** Streaming read api, using byte buffers. */
  ReadByteBuffer,
  /** Streaming read fully api. */
  ReadFully,
  /** Positioned read api. */
  PosRead,
  /** Positioned read fully api. */
  PosReadFully,
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
