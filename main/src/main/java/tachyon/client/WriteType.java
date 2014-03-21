/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.client;

import java.io.IOException;

/**
 * Different write types for a TachyonFile.
 */
public enum WriteType {
  /**
   * Write the file and must cache it.
   */
  MUST_CACHE(1),
  /**
   * Write the file and try to cache it.
   */
  TRY_CACHE(2),
  /**
   * Write the file synchronously to the under fs, and also try to cache it,
   */
  CACHE_THROUGH(3),
  /**
   * Write the file synchronously to the under fs, no cache.
   */
  THROUGH(4),
  /**
   * Write the file asynchronously to the under fs (either must cache or must through).
   */
  ASYNC_THROUGH(5);

  public static WriteType getOpType(String op) throws IOException {
    if (op.equals("MUST_CACHE")) {
      return MUST_CACHE;
    } else if (op.equals("TRY_CACHE")) {
      return TRY_CACHE;
    } else if (op.equals("CACHE_THROUGH")) {
      return CACHE_THROUGH;
    } else if (op.equals("THROUGH")) {
      return THROUGH;
    } else if (op.equals("ASYNC_THROUGH")) {
      return ASYNC_THROUGH;
    }

    throw new IOException("Unknown WriteType : " + op);
  }

  private final int mValue;

  private WriteType(int value) {
    mValue = value;
  }

  public int getValue() {
    return mValue;
  }

  public boolean isAsync() {
    return mValue == ASYNC_THROUGH.mValue;
  }

  public boolean isCache() {
    return (mValue == MUST_CACHE.mValue) || (mValue == CACHE_THROUGH.mValue)
        || (mValue == TRY_CACHE.mValue) || (mValue == ASYNC_THROUGH.mValue);
  }

  public boolean isMustCache() {
    return (mValue == MUST_CACHE.mValue) || (mValue == ASYNC_THROUGH.mValue);
  }

  public boolean isThrough() {
    return (mValue == CACHE_THROUGH.mValue) || (mValue == THROUGH.mValue);
  }
}
