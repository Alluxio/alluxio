/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio;

/**
 * System constants that are determined during runtime
 */
public final class RuntimeConstants {
  static {
    Configuration configuration = new Configuration();
    VERSION = configuration.get(Constants.VERSION);
  }

  /** The version of this Alluxio instance */
  public static final String VERSION;

  /** The relative path to the Alluxio target jar. */
  public static final String ALLUXIO_JAR =
      "target/alluxio-" + VERSION + "-jar-with-dependencies.jar";

  private RuntimeConstants() {
  } // prevent instantiation
}
