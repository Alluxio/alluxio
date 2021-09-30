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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An extension of the {@link ResourceLeakDetector} with custom error messages for use in the
 * Alluxio codebase.
 *
 * @param <T> the type of resource the detector tracks
 */
public class AlluxioResourceLeakDetector<T> extends ResourceLeakDetector<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AlluxioResourceLeakDetector.class);

  static {
    ResourceLeakDetector.Level lev = ResourceLeakDetector.Level.valueOf(
        ConfigurationUtils.getPropertyValue(PropertyKey.LEAK_DETECTOR_LEVEL));
    ResourceLeakDetector.setLevel(lev);
  }

  /**
   * Creates a new instance of the leak detector with the specific resource type and sampling
   * interval.
   *
   * @param resourceType the resource class
   * @param samplingInterval on average, how often a resource should be tracked
   */
  public AlluxioResourceLeakDetector(Class<?> resourceType, int samplingInterval) {
    super(resourceType, samplingInterval);
  }

  /**
   * A traced leak report which includes records of the recent accesses of the particular object and
   * the stacktrace of where the particular object was created.
   *
   * @param resourceType the class of the resource that was leaked
   * @param records the stacktrace of where the leaked resource was created
   */
  @Override
  protected void reportTracedLeak(String resourceType, String records) {
    LOGGER.error("LEAK: {}.close() was not called before resource is garbage-collected. "
        + "See https://docs.alluxio.io/blah/blah/blah for more information about this message.{}",
        resourceType, records);
  }

  /**
   * An untraced leak report where there is no information about recent object accesses nor
   * where the stacktrace of where the object was created.
   *
   * @param resourceType the class name of the resource which was leaked
   */
  @Override
  protected void reportUntracedLeak(String resourceType) {
    LOGGER.error("LEAK: {}.close() was not called before resource is garbage-collected. "
            + "See https://docs.alluxio.io/blah/blah/blah for more information about this message.",
        resourceType);
  }
}
