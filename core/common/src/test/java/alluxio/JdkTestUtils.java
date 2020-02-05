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

import alluxio.util.CommonUtils;

import org.junit.Assume;

/**
 * A utility class for testing JDK versions when running unit and integration tests.
 */
public class JdkTestUtils {

  /**
   * Checks using a Junit {@link Assume} that the current application is running on JDK 8.
   */
  public static void assumeJdk8() {
    Assume.assumeTrue(CommonUtils.isJdk8());
  }

  /**
   * Checks using a Junit {@link Assume} that the current application is running on JDK 11.
   */
  public static void assumeJdk11() {
    Assume.assumeTrue(CommonUtils.isJdk11());
  }

  private JdkTestUtils() {
  }
}
