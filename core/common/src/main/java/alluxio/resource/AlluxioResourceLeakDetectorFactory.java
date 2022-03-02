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

package alluxio.resource;

import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;

/**
 * The class responsible for instantiating {@link AlluxioResourceLeakDetector}.
 */
public class AlluxioResourceLeakDetectorFactory extends ResourceLeakDetectorFactory {

  private static final ResourceLeakDetectorFactory INSTANCE =
      new AlluxioResourceLeakDetectorFactory();

  private final boolean mExitOnLeak = (boolean) ConfigurationUtils.getPropertyValue(
      PropertyKey.LEAK_DETECTOR_EXIT_ON_LEAK);

  /**
   * @return the singleton instance of the {@link AlluxioResourceLeakDetectorFactory}
   */
  public static ResourceLeakDetectorFactory instance() {
    return INSTANCE;
  }

  @Override
  public <T> ResourceLeakDetector<T> newResourceLeakDetector(Class<T> resource,
      int samplingInterval, long maxActive) {
    return newResourceLeakDetector(resource, samplingInterval);
  }

  @Override
  public <T> ResourceLeakDetector<T> newResourceLeakDetector(Class<T> resource,
      int samplingInterval) {
    return new AlluxioResourceLeakDetector<>(resource, samplingInterval, mExitOnLeak);
  }
}
