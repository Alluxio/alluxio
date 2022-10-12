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

package alluxio.master.throttle;

import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterContext;
import alluxio.master.MasterFactory;
import alluxio.master.MasterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link DefaultThrottleMaster} instance.
 */
@ThreadSafe
public final class ThrottleMasterFactory implements MasterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ThrottleMasterFactory.class);

  /**
   * Constructs a new {@link ThrottleMasterFactory}.
   */
  public ThrottleMasterFactory() {}

  @Override
  public boolean isEnabled() {
    return Configuration.getBoolean(PropertyKey.MASTER_THROTTLE_ENABLED);
  }

  @Override
  public String getName() {
    return Constants.THROTTLE_MASTER_NAME;
  }

  @Override
  public DefaultThrottleMaster create(MasterRegistry registry, MasterContext context) {
    if (!isEnabled()) {
      return null;
    }
    LOG.info("Creating {} ", DefaultThrottleMaster.class.getName());
    return new DefaultThrottleMaster(registry, context);
  }
}

