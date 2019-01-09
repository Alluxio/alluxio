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

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Utility methods for Alluxio services.
 */
public final class ServiceUtils {

  /**
   * @return service loader for master factories
   */
  public static synchronized ServiceLoader<MasterFactory> getMasterServiceLoader() {
    return ServiceLoader.load(MasterFactory.class, MasterFactory.class.getClassLoader());
  }

  /**
   * @return the list of master service names
   */
  public static List<String> getMasterServiceNames() {
    List<String> masterServiceNames = new ArrayList<>();
    for (MasterFactory factory : getMasterServiceLoader()) {
      if (factory.isEnabled()) {
        masterServiceNames.add(factory.getName());
      }
    }
    return masterServiceNames;
  }

  private ServiceUtils() {} // prevent instantiation
}
