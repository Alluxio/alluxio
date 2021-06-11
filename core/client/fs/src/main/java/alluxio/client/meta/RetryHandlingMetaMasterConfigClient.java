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

package alluxio.client.meta;

import alluxio.AbstractMasterClient;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GetConfigHashPOptions;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.MetaMasterConfigurationServiceGrpc;
import alluxio.grpc.RemovePathConfigurationPRequest;
import alluxio.grpc.ServiceType;
import alluxio.grpc.SetPathConfigurationPRequest;
import alluxio.grpc.UpdateConfigurationPRequest;
import alluxio.master.MasterClientContext;
import alluxio.wire.ConfigHash;
import alluxio.wire.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the meta master.
 */
@ThreadSafe
public class RetryHandlingMetaMasterConfigClient extends AbstractMasterClient
    implements MetaMasterConfigClient {
  private static final Logger RPC_LOG = LoggerFactory.getLogger(MetaMasterConfigClient.class);
  private MetaMasterConfigurationServiceGrpc.MetaMasterConfigurationServiceBlockingStub mClient =
      null;

  /**
   * Creates a new meta master client.
   *
   * @param conf master client configuration
   */
  public RetryHandlingMetaMasterConfigClient(MasterClientContext conf) {
    super(conf);
  }

  @Override
  protected ServiceType getRemoteServiceType() {
    return ServiceType.META_MASTER_CONFIG_SERVICE;
  }

  @Override
  protected String getServiceName() {
    return Constants.META_MASTER_CONFIG_SERVICE_NAME;
  }

  @Override
  protected long getServiceVersion() {
    return Constants.META_MASTER_CONFIG_SERVICE_VERSION;
  }

  @Override
  protected void afterConnect() {
    mClient = MetaMasterConfigurationServiceGrpc.newBlockingStub(mChannel);
  }

  @Override
  public Configuration getConfiguration(GetConfigurationPOptions options) throws IOException {
    return Configuration.fromProto(retryRPC(() -> mClient.withDeadlineAfter(
        mContext.getClusterConf().getMs(PropertyKey.WORKER_MASTER_PERIODICAL_RPC_TIMEOUT),
        TimeUnit.MILLISECONDS).getConfiguration(options),
        RPC_LOG, "GetConfiguration", "options=%s", options));
  }

  @Override
  public ConfigHash getConfigHash() throws IOException {
    return ConfigHash.fromProto(retryRPC(() -> mClient.withDeadlineAfter(
        mContext.getClusterConf().getMs(PropertyKey.WORKER_MASTER_PERIODICAL_RPC_TIMEOUT),
        TimeUnit.MILLISECONDS).getConfigHash(GetConfigHashPOptions.getDefaultInstance()),
        RPC_LOG, "GetConfigHash", ""));
  }

  @Override
  public void setPathConfiguration(AlluxioURI path, Map<PropertyKey, String> properties)
      throws IOException {
    Map<String, String> props = new HashMap<>();
    properties.forEach((key, value) -> props.put(key.getName(), value));
    retryRPC(() -> mClient.setPathConfiguration(SetPathConfigurationPRequest.newBuilder()
        .setPath(path.getPath()).putAllProperties(props).build()),
        RPC_LOG, "setPathConfiguration", "path=%s,properties=%s", path, properties);
  }

  @Override
  public void removePathConfiguration(AlluxioURI path, Set<PropertyKey> keys) throws IOException {
    Set<String> keySet = new HashSet<>();
    for (PropertyKey key : keys) {
      keySet.add(key.getName());
    }
    retryRPC(() -> mClient.removePathConfiguration(RemovePathConfigurationPRequest.newBuilder()
        .setPath(path.getPath()).addAllKeys(keySet).build()),
        RPC_LOG, "removePathConfiguration", "path=%s,keys=%s", path, keys);
  }

  @Override
  public void removePathConfiguration(AlluxioURI path) throws IOException {
    retryRPC(() -> mClient.removePathConfiguration(RemovePathConfigurationPRequest.newBuilder()
        .setPath(path.getPath()).build()), RPC_LOG, "removePathConfiguration", "path=%s", path);
  }

  @Override
  public Map<PropertyKey, Boolean> updateConfiguration(
      Map<PropertyKey, String> propertiesMap) throws IOException {
    Map<PropertyKey, Boolean> resultMap = new HashMap<>();
    Map<String, String> inputMap = new HashMap<>();
    propertiesMap.forEach((k, v) -> inputMap.put(k.getName(), v));
    retryRPC(
        () -> mClient.updateConfiguration(
            UpdateConfigurationPRequest.newBuilder()
                .putAllProperties(inputMap)
                .build()),
        RPC_LOG, "updateConfiguration", "propertiesMap=%s", propertiesMap)
        .getStatusMap().forEach((k, v) -> {
          resultMap.put(PropertyKey.getOrBuildCustom(k), v);
        });
    return resultMap;
  }
}
