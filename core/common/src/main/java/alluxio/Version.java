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

package alluxio;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The version of the current build.
 */
@ThreadSafe
public final class Version {
  public static final String VERSION;

  private Version() {} // prevent instantiation

  static {
    Configuration configuration = new Configuration();
    VERSION = configuration.get(Constants.TACHYON_VERSION);
  }

  /** The relative path to the Tachyon target jar. */
  public static final String TACHYON_JAR = "target/alluxio-" + VERSION
      + "-jar-with-dependencies.jar";

  /**
   * Prints the version of the current build.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {
    System.out.println("Tachyon version: " + VERSION);
  }
}
