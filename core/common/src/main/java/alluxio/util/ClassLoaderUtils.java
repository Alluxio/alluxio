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

package alluxio.util;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility functions for working with classloaders.
 */

@ThreadSafe
public class ClassLoaderUtils {
  /**
   * Add a path to be loaded.
   * @param path full path
   */
  public static void addURL(String path) throws Exception {
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    try {
      Method method = classLoader.getClass().getDeclaredMethod("addURL", URL.class);
      method.setAccessible(true);
      method.invoke(classLoader, new File(path).toURI().toURL());
    } catch (NoSuchMethodException e) {
      Method method = classLoader.getClass().getDeclaredMethod(
          "appendToClassPathForInstrumentation", String.class);
      method.setAccessible(true);
      method.invoke(classLoader, path);
    }
  }

  private ClassLoaderUtils() {} // prevent instantiation
}
