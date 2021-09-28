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

  @Override
  protected void reportTracedLeak(String resourceType, String records) {
    LOGGER.error("LEAK: {}.close() was not called before resource is garbage-collected. "
        + "See https://docs.alluxio.io/blah/blah/blah for more information about this message.{}",
        resourceType, records);
  }

  @Override
  protected void reportUntracedLeak(String resourceType) {
    LOGGER.error("LEAK: {}.close() was not called before resource is garbage-collected. "
            + "See https://docs.alluxio.io/blah/blah/blah for more information about this message.",
        resourceType);
  }
}
