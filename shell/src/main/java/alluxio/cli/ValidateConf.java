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

import alluxio.annotation.PublicApi;
import alluxio.conf.InstancedConfiguration;
import alluxio.util.ConfigurationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Validate the Alluxio configuration.
 */
@ThreadSafe
@PublicApi
public final class ValidateConf {
  private static final Logger LOG = LoggerFactory.getLogger(ValidateConf.class);

  /**
   * Console program that validates the configuration.
   *
   * @param args there are no arguments needed
   */
  public static void main(String[] args) {
    LOG.info("Validating configuration.");
    try {
      new InstancedConfiguration(ConfigurationUtils.defaults()).validate();
      LOG.info("Configuration is valid.");
    } catch (IllegalStateException e) {
      LOG.error("Configuration is invalid", e);
      System.exit(-1);
    }
    System.exit(0);
  }

  private ValidateConf() {} // prevent instantiation.
}
