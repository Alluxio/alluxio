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

package alluxio.util;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.SystemUtils;

/**
 * OS related utility functions.
 */
@ThreadSafe
public final class OSUtils {
  /** The OS name. */
  public static final String OS_NAME = System.getProperty("os.name");
  /** The processor bit. */
  public static final String PROCESSOR_BIT = System.getProperty("os.arch");
  /** The java vendor name used in this platform. */
  public static final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");
  /** Indicates the current java vendor is IBM java or not. */
  public static final boolean IBM_JAVA = JAVA_VENDOR_NAME.contains("IBM");

  /**
   * @return true if current processor is 64 bit
   */
  public static boolean is64Bit() {
    return OSUtils.PROCESSOR_BIT.contains("64");
  }

  /**
   * @return true if current OS is Windows
   */
  public static boolean isWindows() {
    return SystemUtils.IS_OS_WINDOWS;
  }

  /**
   * @return true if current OS is AIX
   */
  public static boolean isAIX() {
    return OSUtils.OS_NAME.equals("AIX");
  }

  private OSUtils() {} // prevent instantiation
}
