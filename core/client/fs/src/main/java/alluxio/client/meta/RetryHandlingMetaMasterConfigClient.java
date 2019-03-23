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
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.MetaMasterConfigurationServiceGrpc;
import alluxio.grpc.ServiceType;
import alluxio.grpc.SetPathConfigurationPRequest;
import alluxio.master.MasterClientContext;
import alluxio.wire.Configuration;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper for the gRPC client to interact with the meta master.
 */
@ThreadSafe
public class RetryHandlingMetaMasterConfigClient extends AbstractMasterClient
    implements MetaMasterConfigClient {
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
  public Configuration getConfiguration() throws IOException {
    return Configuration.fromProto(retryRPC(() ->
        mClient.getConfiguration(GetConfigurationPOptions.getDefaultInstance())));
  }

  @Override
  public void setPathConfiguration(String path, PropertyKey key, String value) throws IOException {
    retryRPC(() -> mClient.setPathConfiguration(SetPathConfigurationPRequest.newBuilder()
        .setPath(path).setKey(key.getName()).setValue(value).build()));
  }
}
