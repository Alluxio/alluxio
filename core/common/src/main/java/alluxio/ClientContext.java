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

package alluxio;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.util.ConfigurationUtils;

import javax.annotation.Nullable;
import javax.security.auth.Subject;

/**
 * A ClientContext contains information about a security subject and Alluxio configuration which
 * is used to perform client operations in an Alluxio cluster.
 */
public class ClientContext {

  private final AlluxioConfiguration mConf;
  private final Subject mSubject;

  /**
   * A client context with information about the subject and configuration of the client.
   *
   * @param subject The security subject to use
   * @param alluxioConf The {@link AlluxioConfiguration} to use. If null, the site property defaults
   * will be loaded
   * @return A new client context with the specified properties and subject
   */
  public static ClientContext create(@Nullable Subject subject,
      @Nullable AlluxioConfiguration alluxioConf) {
    return new ClientContext(subject, alluxioConf);
  }

  /**
   * @param alluxioConf The specified {@link AlluxioConfiguration} to use
   * @return the client context with the given properties and an empty subject
   */
  public static ClientContext create(@Nullable AlluxioConfiguration alluxioConf) {
    return new ClientContext(null, alluxioConf);
  }

  /**
   * @return a new {@link ClientContext} with values loaded from the alluxio-site properties and
   * an empty subject.
   */
  public static ClientContext create() {
    return new ClientContext(null, null);
  }

  private ClientContext(@Nullable Subject subject, @Nullable AlluxioConfiguration alluxioConf) {
    mSubject = subject;
    // Copy the properties so that future modification doesn't affect this ClientContext.
    if (alluxioConf != null) {
      mConf = new InstancedConfiguration(alluxioConf.getProperties().copy(),
          alluxioConf.clusterDefaultsLoaded());
    } else {
      mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
    }
  }

  /**
   * @return the {@link AlluxioConfiguration} backing this context
   */
  public AlluxioConfiguration getConf() {
    return mConf;
  }

  /**
   * @return The Subject backing this context
   */
  public Subject getSubject() {
    return mSubject;
  }
}
