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

import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.path.PathConfiguration;
import alluxio.conf.path.PrefixPathConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.util.ConfigurationUtils;

import com.google.common.annotations.VisibleForTesting;

import java.net.InetSocketAddress;

import javax.annotation.Nullable;
import javax.security.auth.Subject;

/**
 * A {@link ClientContext} contains information required and pertaining to making network
 * connections and performing operations with remote Alluxio processes. The {@link ClientContext}
 * should only contain the information which is necessary to make those connections.
 *
 * A {@link ClientContext} are not expensive objects to create, however it is important to be
 * aware that if the configuration with which the instance is created does not have the cluster
 * default configuration loaded that any new clients which use the context will need to load the
 * cluster defaults upon connecting to the Alluxio master.
 *
 * Ideally only a single {@link ClientContext} should be needed when initializing an application.
 * This will use as few network resources as possible.
 */
public class ClientContext {
  private volatile AlluxioConfiguration mConf;
  private volatile PathConfiguration mPathConf;
  private final Subject mSubject;

  /**
   * A client context with information about the subject and configuration of the client.
   *
   * @param subject the security subject to use
   * @param alluxioConf the {@link AlluxioConfiguration} to use. If null, the site property defaults
   *     will be loaded
   * @return a new client context with the specified properties and subject
   */
  public static ClientContext create(Subject subject, AlluxioConfiguration alluxioConf) {
    return new ClientContext(subject, alluxioConf);
  }

  /**
   * @param alluxioConf the specified {@link AlluxioConfiguration} to use
   * @return the client context with the given properties and an empty subject
   */
  public static ClientContext create(AlluxioConfiguration alluxioConf) {
    return new ClientContext(null, alluxioConf);
  }

  /**
   * @return a new {@link ClientContext} with values loaded from the alluxio-site properties and
   * an empty subject.
   */
  public static ClientContext create() {
    return new ClientContext(null, null);
  }

  /**
   * This constructor does not create a copy of the configuration.
   */
  protected ClientContext(ClientContext ctx) {
    mSubject = ctx.getSubject();
    mConf = ctx.getConf();
    mPathConf = ctx.getPathConf();
  }

  private ClientContext(@Nullable Subject subject, @Nullable AlluxioConfiguration alluxioConf) {
    if (subject != null) {
      mSubject = subject;
    } else {
      mSubject = new Subject();
    }
    // Copy the properties so that future modification doesn't affect this ClientContext.
    if (alluxioConf != null) {
      mConf = new InstancedConfiguration(alluxioConf.copyProperties(),
          alluxioConf.clusterDefaultsLoaded());
    } else {
      mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
    }
    mPathConf = new PrefixPathConfiguration();
  }

  /**
   * This method will attempt to load the cluster and path level configuration defaults and update
   * the configuration if necessary.
   *
   * This method should be synchronized so that concurrent calls to it don't continually overwrite
   * the previous configuration. The cluster defaults should only ever need to be updated once
   * per {@link ClientContext} reference.
   *
   * @param address the address to load cluster defaults from
   * @throws AlluxioStatusException
   */
  @VisibleForTesting
  public synchronized void updateConfigurationDefaults(InetSocketAddress address)
      throws AlluxioStatusException {
    Pair<AlluxioConfiguration, PathConfiguration> conf =
        ConfigurationUtils.loadClusterAndPathDefaults(address, mConf, mPathConf);
    mConf = conf.getFirst();
    mPathConf = conf.getSecond();
  }

  /**
   * @return the cluster level configuration backing this context
   */
  public AlluxioConfiguration getConf() {
    return mConf;
  }

  /**
   * @return the path level configuration backing this context
   */
  public PathConfiguration getPathConf() {
    return mPathConf;
  }

  /**
   * @return the Subject backing this context
   */
  public Subject getSubject() {
    return mSubject;
  }
}
