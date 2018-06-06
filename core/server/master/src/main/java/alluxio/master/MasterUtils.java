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

package alluxio.master;

import alluxio.ServiceUtils;
import alluxio.util.CommonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * This class encapsulates the different master services that are configured to run.
 */
final class MasterUtils {
  private MasterUtils() {}  // prevent instantiation

  /**
   * Creates all the masters and registers them to the master registry.
   *
   * @param registry the master registry
   * @param context master context
   */
  public static void createMasters(MasterRegistry registry, MasterContext context) {
    List<Callable<Void>> callables = new ArrayList<>();
    for (final MasterFactory factory : ServiceUtils.getMasterServiceLoader()) {
      callables.add(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          if (factory.isEnabled()) {
            factory.create(registry, context);
          }
          return null;
        }
      });
    }
    try {
      CommonUtils.invokeAll(callables, 10, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start masters", e);
    }
  }
}
