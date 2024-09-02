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

import com.google.common.annotations.VisibleForTesting;

/**
 * Used for injecting faults in Alluxio tests.
 * Calls into this are a no-op in production code.
 */
@VisibleForTesting
public class AlluxioFaultInjector {
  private static AlluxioFaultInjector instance = new AlluxioFaultInjector();

  public static AlluxioFaultInjector get() {
    return instance;
  }

  public static void set(AlluxioFaultInjector injector) {
    instance = injector;
  }

  /**
   * Used as a hook to inject intercept when evicting unused resource from pool.
   */
  public void blockUtilAllocatedNewResource() {

  }
}
