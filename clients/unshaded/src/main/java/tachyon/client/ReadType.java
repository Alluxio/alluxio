/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client;

/**
 * Different read types for a TachyonFile.
 */
public enum ReadType {
  /**
   * Read the file and but do not cache it explicitly.
   */
  NO_CACHE(1),
  /**
   * Read the file and cache it.
   */
  CACHE(2);

  /**
   * If the block is on local Tachyon space but not on top storage layer, promote the block to top
   * storage layer after reading.
   *
   * TODO(calvin): Enable this again when lock upgrading is implemented in the worker.
   */
  //CACHE_PROMOTE(3);

  private final int mValue;

  ReadType(int value) {
    mValue = value;
  }

  /**
   * Return the value of the read type
   *
   * @return the read type value
   */
  public int getValue() {
    return mValue;
  }

  /**
   * @return true if the read type is CACHE, false otherwise
   *
   * TODO(calvin): Add CACHE_PROMOTE back when it is enabled again.
   */
  public boolean isCache() {
    return mValue == CACHE.mValue;
    // return mValue == CACHE.mValue || mValue == CACHE_PROMOTE.mValue;
  }

  /**
   * @return true if the read type is CACHE_PROMOTE, false otherwise
   *
   * TODO(calvin): Add this function back when CACHE_PROMOTE is enabled again.
   */
//  public boolean isPromote() {
//    return mValue == CACHE_PROMOTE.mValue;
//  }
}
