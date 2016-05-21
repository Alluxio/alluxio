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

package alluxio.cli;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.util.ConfigurationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Validate the Alluxio configuration.
 */
// TODO(binfan): move property names from Constants to separate class and validate conf from there.
@ThreadSafe
public final class ValidateConf {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Console program that validates the configuration.
   *
   * @param args there are no arguments needed
   */
  public static void main(String[] args) {
    int ret = 0;
    LOG.info("Validating client configuration.");
    if (ConfigurationUtils.validateConf(Configuration.createClientConf())) {
      LOG.info("All client configuration entries are valid.");
    } else {
      LOG.info("Client configuration has invalid entries.");
      ret = -1;
    }

    LOG.info("Validating sever configuration.");
    if (ConfigurationUtils.validateConf(Configuration.createServerConf())) {
      LOG.info("All server configuration entries are valid.");
    } else {
      LOG.info("Server configuration has invalid entries.");
      ret = -1;
    }
    System.exit(ret);
  }

  private ValidateConf() {} // prevent instantiation.
}
