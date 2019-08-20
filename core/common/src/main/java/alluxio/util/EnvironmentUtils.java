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

import alluxio.util.io.FileUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utilities to detect environment Alluxio is running in.
 */
@ThreadSafe
public final class EnvironmentUtils {
  private static final Logger LOG = LoggerFactory.getLogger(EnvironmentUtils.class);

  /**
   * Utility to detect the docker deployment environment.
   *
   * @return true, if running on docker
   */
  @SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
  public static boolean isDocker() {
    return FileUtils.exists("/.dockerenv");
  }

  /**
   * Utility to detect the k8s deployment environment.
   *
   * @return true, if running on k8s
   */
  public static boolean isKubernetes() {
    return System.getenv("KUBERNETES_SERVICE_HOST") != null;
  }

  private EnvironmentUtils() {} // prevent instantiation
}
