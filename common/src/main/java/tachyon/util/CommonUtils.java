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

package tachyon.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common utilities shared by all components in Tachyon.
 */
public final class CommonUtils {
  private static final Logger LOG = LoggerFactory.getLogger("");

  public static long getCurrentMs() {
    return System.currentTimeMillis();
  }

  public static <T> String listToString(List<T> list) {
    StringBuilder sb = new StringBuilder();
    for (T s : list) {
      sb.append(s).append(" ");
    }
    return sb.toString();
  }

  public static String[] toStringArray(ArrayList<String> src) {
    String[] ret = new String[src.size()];
    return src.toArray(ret);
  }

  public static void sleepMs(Logger logger, long timeMs) {
    sleepMs(logger, timeMs, false);
  }

  public static void sleepMs(Logger logger, long timeMs, boolean shouldInterrupt) {
    try {
      Thread.sleep(timeMs);
    } catch (InterruptedException e) {
      logger.warn(e.getMessage(), e);
      if (shouldInterrupt) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Creates new instance of a class by calling a constructor that receives ctorClassArgs arguments
   *
   * @param cls the class to create
   * @param ctorClassArgs parameters type list of the constructor to initiate, if null default
   *        constructor will be called
   * @param ctorArgs the arguments to pass the constructor
   * @return new class object or null if not successful
   */
  public static <T> T createNewClassInstance(Class<T> cls, Class<?>[] ctorClassArgs,
      Object[] ctorArgs) throws InstantiationException, IllegalAccessException,
      NoSuchMethodException, SecurityException, InvocationTargetException {
    if (ctorClassArgs == null) {
      return cls.newInstance();
    }
    Constructor<T> ctor = cls.getConstructor(ctorClassArgs);
    return ctor.newInstance(ctorArgs);
  }
}
