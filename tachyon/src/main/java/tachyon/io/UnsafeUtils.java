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
package tachyon.io;

import java.lang.reflect.Field;

import sun.misc.Unsafe;
import tachyon.util.CommonUtils;

/**
 * Utilities for unsafe operations.
 */
public class UnsafeUtils {
  public static Unsafe getUnsafe()
      throws NoSuchFieldException, SecurityException, IllegalArgumentException,
      IllegalAccessException {
    Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
    theUnsafe.setAccessible(true);
    return (Unsafe) theUnsafe.get(null);
  }

  public static int sByteArrayBaseOffset;

  static {
    try {
      sByteArrayBaseOffset = getUnsafe().arrayBaseOffset(byte[].class);
    } catch (NoSuchFieldException | SecurityException | IllegalArgumentException
        | IllegalAccessException e) {
      CommonUtils.runtimeException(e);
    }
  }
}
