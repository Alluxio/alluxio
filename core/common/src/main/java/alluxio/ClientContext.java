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
import alluxio.conf.path.PathConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.GetConfigurationPResponse;
import alluxio.util.ConfigurationUtils;

import java.net.InetSocketAddress;
import java.util.HashMap;

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
 * Path level configuration may not be needed for any ClientContext, it is currently only used in
 * BaseFileSystem, so it is initially lazily loaded by FileSystemContext when it's needed.
 *
 * Ideally only a single {@link ClientContext} should be needed when initializing an application.
 * This will use as few network resources as possible.
 */
public class ClientContext {
  private volatile AlluxioConfiguration mClusterConf;
  private volatile String mClusterConfHash;
  private volatile PathConfiguration mPathConf;
  private volatile String mPathConfHash;
  private volatile boolean mIsPathConfLoaded = false;
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
    mClusterConf = ctx.getClusterConf();
    mPathConf = ctx.getPathConf();
    mClusterConfHash = ctx.getClusterConfHash();
    mPathConfHash = ctx.getPathConfHash();
  }

  private ClientContext(@Nullable Subject subject, @Nullable AlluxioConfiguration alluxioConf) {
    if (subject != null) {
      mSubject = subject;
    } else {
      mSubject = new Subject();
    }
    // Copy the properties so that future modification doesn't affect this ClientContext.
    if (alluxioConf != null) {
      mClusterConf = new InstancedConfiguration(alluxioConf.copyProperties(),
          alluxioConf.clusterDefaultsLoaded());
      mClusterConfHash = alluxioConf.hash();
    } else {
      mClusterConf = new InstancedConfiguration(ConfigurationUtils.defaults());
      mClusterConfHash = mClusterConf.hash();
    }
    mPathConf = PathConfiguration.create(new HashMap<>());
  }

  /**
   * This method will load the cluster and path level configuration defaults and update
   * the configuration in one RPC.
   *
   * This method should be synchronized so that concurrent calls to it don't continually overwrite
   * the previous configuration.
   *
   * The cluster defaults are updated per connection establishment, or when cluster defaults
   * updates are detected on client side.
   *
   * @param address the address to load cluster defaults from
   * @throws AlluxioStatusException
   */
  public synchronized void updateClusterAndPathConf(InetSocketAddress address)
      throws AlluxioStatusException {
    GetConfigurationPResponse response = ConfigurationUtils.loadConfiguration(address,
        mClusterConf, false, false);
    AlluxioConfiguration clusterConf = ConfigurationUtils.getClusterConf(response, mClusterConf);
    PathConfiguration pathConf = ConfigurationUtils.getPathConf(response, mClusterConf);

    mClusterConf = clusterConf;
    mClusterConfHash = response.getClusterConfigHash();
    mPathConf = pathConf;
    mPathConfHash = response.getPathConfigHash();
    mIsPathConfLoaded = true;
  }

  /**
   * Updates cluster level configuration only.
   *
   * @param address the meta master address
   * @throws AlluxioStatusException
   */
  public synchronized void updateClusterConf(InetSocketAddress address)
      throws AlluxioStatusException {
    GetConfigurationPResponse response = ConfigurationUtils.loadConfiguration(address,
        mClusterConf, false, true);
    AlluxioConfiguration clusterConf = ConfigurationUtils.getClusterConf(response, mClusterConf);

    mClusterConf = clusterConf;
    mClusterConfHash = response.getClusterConfigHash();
  }

  /**
   * Updates path level configuration only.
   *
   * @param address the meta master address
   * @throws AlluxioStatusException
   */
  public synchronized void updatePathConf(InetSocketAddress address)
      throws AlluxioStatusException {
    GetConfigurationPResponse response = ConfigurationUtils.loadConfiguration(address,
        mClusterConf, true, false);
    PathConfiguration pathConf = ConfigurationUtils.getPathConf(response, mClusterConf);

    mPathConf = pathConf;
    mPathConfHash = response.getPathConfigHash();
    mIsPathConfLoaded = true;
  }

  /**
   * Loads path level configuration if not loaded from meta master yet.
   *
   * @param address meta master address
   * @throws AlluxioStatusException
   */
  public synchronized void loadPathConfIfNotLoaded(InetSocketAddress address)
      throws AlluxioStatusException{
    if (!mIsPathConfLoaded) {
      updatePathConf(address);
      mIsPathConfLoaded = true;
    }
  }

  /**
   * @return the cluster level configuration backing this context
   */
  public AlluxioConfiguration getClusterConf() {
    return mClusterConf;
  }

  /**
   * @return the path level configuration backing this context
   */
  public PathConfiguration getPathConf() {
    return mPathConf;
  }

  /**
   * @return hash of cluster level configuration
   */
  public String getClusterConfHash() {
    return mClusterConfHash;
  }

  /**
   * @return hash of path level configuration
   */
  public String getPathConfHash() {
    return mPathConfHash;
  }

  /**
   * @return the Subject backing this context
   */
  public Subject getSubject() {
    return mSubject;
  }
}
