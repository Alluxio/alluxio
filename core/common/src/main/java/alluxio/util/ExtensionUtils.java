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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;

import java.io.File;
import java.io.FileFilter;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility functions for working with extensions.
 */
@ThreadSafe
public final class ExtensionUtils {

  private static final File[] EMPTY_EXTENSIONS_LIST = new File[0];

  /**
   * List extension jars from the configured extensions directory.
   *
   * @return an array of files (one file per jar)
   */
  public static File[] listExtensions() {
    String extensionDir = Configuration.get(PropertyKey.EXTENSIONS_DIR);
    File[] extensions = new File(extensionDir).listFiles(new FileFilter() {
      public boolean accept(File file) {
        return file.getPath().toLowerCase().endsWith(Constants.EXTENSION_JAR);
      }
    });
    if (extensions == null) {
      // Directory does not exist
      return EMPTY_EXTENSIONS_LIST;
    }
    return extensions;
  }

  private ExtensionUtils() {} // prevent instantiation
}
