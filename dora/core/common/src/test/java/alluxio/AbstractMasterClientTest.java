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

import alluxio.conf.Configuration;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.ServiceType;
import alluxio.master.MasterClientContext;
import alluxio.master.MasterInquireClient;
import alluxio.master.selectionpolicy.MasterSelectionPolicy;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link AbstractMasterClientTest}.
 */
public class AbstractMasterClientTest {
  static class TestAbstractClient extends AbstractMasterClient {

    public TestAbstractClient(MasterClientContext clientConf, MasterSelectionPolicy connectMode) {
      super(clientConf, connectMode);
    }

    @Override
    protected ServiceType getRemoteServiceType() {
      return ServiceType.FILE_SYSTEM_MASTER_CLIENT_SERVICE;
    }

    @Override
    protected String getServiceName() {
      return "SERVICE_NAME";
    }

    @Override
    protected long getServiceVersion() {
      return 0;
    }
  }

  MasterInquireClient mMockMasterInquireClient;
  MasterClientContext mMockMasterClientContext;

  List<InetSocketAddress> mAddress = Arrays.asList(
      new InetSocketAddress("114.5.1.4", 810),
      new InetSocketAddress("114.5.1.4", 811),
      new InetSocketAddress("114.5.1.4", 812)
  );
  int mPrimaryMasterIndex = 0;

  @Before
  public void setup() throws UnavailableException {
    mPrimaryMasterIndex = 0;
    mMockMasterInquireClient = Mockito.mock(MasterInquireClient.class);
    mMockMasterClientContext = Mockito.mock(MasterClientContext.class);
    Mockito.when(mMockMasterClientContext.getMasterInquireClient())
        .thenReturn(mMockMasterInquireClient);
    Mockito.when(mMockMasterClientContext.getConfMasterInquireClient())
        .thenReturn(mMockMasterInquireClient);
    Mockito.when(mMockMasterClientContext.getClusterConf()).thenReturn(
        Configuration.modifiableGlobal());
    Mockito.when(mMockMasterInquireClient.getMasterRpcAddresses()).thenReturn(mAddress);
    Mockito.when(mMockMasterInquireClient.getPrimaryRpcAddress()).thenAnswer(
        (invocation) -> mAddress.get(mPrimaryMasterIndex)
    );
  }

  @Test
  public void primaryMaster() throws UnavailableException {
    AbstractMasterClient client = new TestAbstractClient(
        mMockMasterClientContext,
        MasterSelectionPolicy.Factory.primaryMaster()
    );
    Assert.assertEquals(mAddress.get(mPrimaryMasterIndex), client.getRemoteSockAddress());
    Assert.assertEquals(mAddress.get(mPrimaryMasterIndex), client.getConfAddress());

    mPrimaryMasterIndex = 1;

    // To simulate the client.disconnect() then client.connect()
    client.afterDisconnect();
    client.mServerAddress = null;

    Assert.assertEquals(mAddress.get(mPrimaryMasterIndex), client.getRemoteSockAddress());
    Assert.assertEquals(mAddress.get(mPrimaryMasterIndex), client.getConfAddress());
  }

  @Test
  public void anyStandbyMaster() throws UnavailableException {
    AbstractMasterClient client =
        new TestAbstractClient(
            mMockMasterClientContext, MasterSelectionPolicy.Factory.anyStandbyMaster());
    Assert.assertNotEquals(mAddress.get(mPrimaryMasterIndex), client.getRemoteSockAddress());
    Assert.assertEquals(mAddress.get(mPrimaryMasterIndex), client.getConfAddress());

    mPrimaryMasterIndex = 1;

    // To simulate the client.disconnect() then client.connect()
    client.afterDisconnect();
    client.mServerAddress = null;

    Assert.assertNotEquals(mAddress.get(mPrimaryMasterIndex), client.getRemoteSockAddress());
    Assert.assertEquals(mAddress.get(mPrimaryMasterIndex), client.getConfAddress());
  }

  @Test
  public void anyMaster() throws UnavailableException {
    AbstractMasterClient client =
        new TestAbstractClient(mMockMasterClientContext, MasterSelectionPolicy.Factory.anyMaster());
    Assert.assertTrue(mAddress.contains(client.getRemoteSockAddress()));
    Assert.assertEquals(mAddress.get(mPrimaryMasterIndex), client.getConfAddress());

    mPrimaryMasterIndex = 1;

    // To simulate the client.disconnect() then client.connect()
    client.afterDisconnect();
    client.mServerAddress = null;

    Assert.assertTrue(mAddress.contains(client.getRemoteSockAddress()));
    Assert.assertEquals(mAddress.get(mPrimaryMasterIndex), client.getConfAddress());
  }

  @Test
  public void specificMaster() throws UnavailableException {
    int masterIndex = 2;
    AbstractMasterClient client = new TestAbstractClient(
        mMockMasterClientContext,
        MasterSelectionPolicy.Factory.specifiedMaster(mAddress.get(masterIndex))
    );
    Assert.assertEquals(mAddress.get(masterIndex), client.getRemoteSockAddress());
    Assert.assertEquals(mAddress.get(mPrimaryMasterIndex), client.getConfAddress());
  }
}
