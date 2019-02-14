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

package alluxio;

import javax.annotation.concurrent.ThreadSafe;

/**
 * System constants that are determined during runtime.
 */
// Note: PropertyKey depends on this class, so this class shouldn't depend on any classes that have
// a dependency on PropertyKey.
@ThreadSafe
public final class RuntimeConstants {
  /** The version of this Alluxio instance. */
  public static final String VERSION = ProjectConstants.VERSION;

  static {
    if (VERSION.endsWith("SNAPSHOT")) {
      ALLUXIO_DOCS_URL = "https://www.alluxio.org/docs/master";
      ALLUXIO_JAVADOC_URL = "https://www.alluxio.org/javadoc/master";
    } else {
      String[] majorMinor = VERSION.split("\\.");
      ALLUXIO_DOCS_URL =
          String.format("https://www.alluxio.org/docs/%s.%s", majorMinor[0], majorMinor[1]);
      ALLUXIO_JAVADOC_URL =
          String.format("https://www.alluxio.org/javadoc/%s.%s", majorMinor[0], majorMinor[1]);
    }
  }

  /** The relative path to the Alluxio target jar. */
  public static final String ALLUXIO_JAR =
      "target/alluxio-" + VERSION + "-jar-with-dependencies.jar";

  /** The URL of Alluxio documentation for this version on project web site. */
  public static final String ALLUXIO_DOCS_URL;

  /** The URL of Alluxio javadoc documentation. */
  public static final String ALLUXIO_JAVADOC_URL;

  /** The URL of Alluxio debugging documentation. */
  public static final String ALLUXIO_DEBUG_DOCS_URL = ALLUXIO_DOCS_URL + "/en/Debugging-Guide.html";

  private RuntimeConstants() {} // prevent instantiation
}
