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

package alluxio.master;

import alluxio.exception.status.UnavailableException;
import alluxio.uri.Authority;
import alluxio.uri.EmbeddedLogicalAuthority;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * EmbeddedLogicalMasterInquireClient is for EmbeddedLogical EmbeddedLogicalAuthority.
 */
public class EmbeddedLogicalMasterInquireClient implements MasterInquireClient {

  private static final Logger LOG = LoggerFactory.getLogger(
      EmbeddedLogicalMasterInquireClient.class);

  //Is designed to be compatible with previous logic
  private MasterInquireClient mCompatibleClient;
  private Map<String, MasterInquireClient> mNameserviceToMasterInquireClient = new HashMap<>();

  private EmbeddedLogicalMasterInquireClient() {
  }

  /**
   * Gets the client.
   *
   * @param nameserviceToMasterInquireClient the map that can get masterInquireClient from logic
   *                                         name
   * @param defaultMasterInquireClient is designed to be compatible with previous logic
   * @return EmbeddedLogicalMasterInquireClient an instance of EmbeddedLogicalMasterInquireClient
   */
  public static EmbeddedLogicalMasterInquireClient getClient(
      Map<String, MasterInquireClient> nameserviceToMasterInquireClient,
      MasterInquireClient defaultMasterInquireClient) {
    EmbeddedLogicalMasterInquireClient client = new EmbeddedLogicalMasterInquireClient();
    client.mNameserviceToMasterInquireClient = nameserviceToMasterInquireClient;
    client.mCompatibleClient = defaultMasterInquireClient;
    return client;
  }

  @Override
  public InetSocketAddress getPrimaryRpcAddress() throws UnavailableException {
    return mCompatibleClient.getPrimaryRpcAddress();
  }

  @Override
  public InetSocketAddress getPrimaryRpcAddress(Authority authority) throws UnavailableException {
    if (authority == null) {
      return getPrimaryRpcAddress();
    }
    MasterInquireClient client = getMasterInquireClient(authority);
    if (client == null) {
      return null;
    }
    return client.getPrimaryRpcAddress();
  }

  private MasterInquireClient getMasterInquireClient(Authority authority)
      throws UnavailableException {
    if (!(authority instanceof EmbeddedLogicalAuthority)) {
      return null;
    }
    EmbeddedLogicalAuthority embeddedLogicalAuthority = (EmbeddedLogicalAuthority) authority;
    MasterInquireClient client = mNameserviceToMasterInquireClient.get(
        embeddedLogicalAuthority.getLogicalName());
    if (client == null) {
      throw new UnavailableException(String.format(
          "Failed to determine primary master rpc address by logicName %s",
          embeddedLogicalAuthority.getLogicalName()));
    }
    return client;
  }

  @Override
  public List<InetSocketAddress> getMasterRpcAddresses() throws UnavailableException {
    return mCompatibleClient.getMasterRpcAddresses();
  }

  @Override
  public List<InetSocketAddress> getMasterRpcAddresses(Authority authority)
      throws UnavailableException {
    if (authority == null) {
      return getMasterRpcAddresses();
    }
    MasterInquireClient client = getMasterInquireClient(authority);
    if (client == null) {
      return null;
    }
    return client.getMasterRpcAddresses();
  }

  @Override
  public ConnectDetails getConnectDetails() {
    return mCompatibleClient.getConnectDetails();
  }

  /**
   * Details used to connect to the Alluxio delegated to mDefaultMasterInquireClient which is
   * designed to be compatible with previous logic.
   */
  public static class EmbeddedLogicalMasterConnectDetails implements ConnectDetails {

    private final MasterInquireClient mDefaultMasterInquireClient;

    /**
     * @param masterInquireClient the default MasterInquireClient
     */
    public EmbeddedLogicalMasterConnectDetails(MasterInquireClient masterInquireClient) {
      mDefaultMasterInquireClient = masterInquireClient;
    }

    @Override
    public Authority toAuthority() {
      return mDefaultMasterInquireClient.getConnectDetails().toAuthority();
    }

    @Override
    public boolean equals(Object o) {
      return mDefaultMasterInquireClient.getConnectDetails().equals(o);
    }

    @Override
    public int hashCode() {
      return mDefaultMasterInquireClient.getConnectDetails().hashCode();
    }

    @Override
    public String toString() {
      return toAuthority().toString();
    }
  }
}
