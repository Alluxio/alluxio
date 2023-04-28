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

package alluxio.wire;

import alluxio.annotation.PublicApi;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Types for file system commands.
 */
@PublicApi
@ThreadSafe
public enum CommandType {
  /**
   * Unknown command.
   */
  UNKNOWN(0),
  /**
   * No op command.
   */
  NOTHING(1),
  /**
   * Ask worker to re-register.
   */
  REGISTER(2),
  /**
   * Ask worker to free files.
   */
  FREE(3),
  /**
   * Ask worker to delete files.
   */
  DELETE(4),
  /**
   * Ask worker to persist a file.
   */
  PERSIST(5),
  ;

  private final int mValue;

  CommandType(int value) {
    mValue = value;
  }

  /**
   * @return the integer value of {@link CommandType}
   */
  public int getValue() {
    return mValue;
  }
}
