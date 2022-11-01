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

package alluxio.client.block.policy;

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.util.logging.SamplingLogger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alias for {@link CapacityBasedRandomPolicy} kept for backwards compatibility.
 *
 * @deprecated in favor of {@link CapacityBasedRandomPolicy}
 */
//todo(bowen): remove in 3.0
@Deprecated
public final class CapacityBaseRandomPolicy extends CapacityBasedRandomPolicy {
  private static final Logger LOG = new SamplingLogger(
      LoggerFactory.getLogger(CapacityBaseRandomPolicy.class), Constants.MINUTE_MS);

  /**
   * Alias for {@link CapacityBasedRandomPolicy#CapacityBasedRandomPolicy(AlluxioConfiguration)}.
   *
   * @param conf Alluxio configuration
   * @deprecated in favor of {@link CapacityBasedRandomPolicy}
   */
  @Deprecated
  public CapacityBaseRandomPolicy(AlluxioConfiguration conf) {
    super(conf);
    LOG.warn("This class name contains a typo and will be removed in 3.0. "
        + "Use {} instead.", CapacityBasedRandomPolicy.class.getName());
  }
}
