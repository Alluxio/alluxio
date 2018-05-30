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

import alluxio.Constants;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.exception.status.UnavailableException;
import alluxio.network.thrift.ThriftUtils;
import alluxio.retry.RetryPolicy;
import alluxio.security.authentication.TransportProvider;

import com.google.common.base.Objects;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Supplier;

import javax.annotation.Nullable;

/**
 * PollingMasterInquireClient finds the address of the primary master by polling a list of master
 * addresses to see if their RPC servers are serving. This works because only primary masters serve
 * RPCs.
 */
public class PollingMasterInquireClient implements MasterInquireClient {
  private static final Logger LOG = LoggerFactory.getLogger(PollingMasterInquireClient.class);

  private final List<InetSocketAddress> mMasterAddresses;
  private final Supplier<RetryPolicy> mRetryPolicySupplier;

  /**
   * @param masterAddresses the potential master addresses
   * @param retryPolicySupplier the retry policy supplier
   */
  public PollingMasterInquireClient(List<InetSocketAddress> masterAddresses,
      Supplier<RetryPolicy> retryPolicySupplier) {
    mMasterAddresses = masterAddresses;
    mRetryPolicySupplier = retryPolicySupplier;
  }

  @Override
  public InetSocketAddress getPrimaryRpcAddress() throws UnavailableException {
    RetryPolicy retry = mRetryPolicySupplier.get();
    while (retry.attempt()) {
      InetSocketAddress address = getAddress();
      if (address != null) {
        return address;
      }
    }
    throw new UnavailableException(String.format(
        "Failed to determine primary master rpc address after polling each of %s %d times",
        mMasterAddresses, retry.getAttemptCount()));
  }

  @Nullable
  private InetSocketAddress getAddress() {
    // Iterate over the masters and try to connect to each of their RPC ports.
    for (InetSocketAddress address : mMasterAddresses) {
      try {
        LOG.debug("Checking whether {} is listening for RPCs", address);
        pingMetaService(address);
        LOG.debug("Successfully connected to {}", address);
        return address;
      } catch (TTransportException e) {
        LOG.debug("Failed to connect to {}", address);
        continue;
      } catch (UnauthenticatedException e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  private void pingMetaService(InetSocketAddress address)
      throws UnauthenticatedException, TTransportException {
    TTransport transport = TransportProvider.Factory.create().getClientTransport(address);
    TProtocol protocol =
        ThriftUtils.createThriftProtocol(transport, Constants.META_MASTER_SERVICE_NAME);
    protocol.getTransport().open();
    protocol.getTransport().close();
  }

  @Override
  public List<InetSocketAddress> getMasterRpcAddresses() {
    return mMasterAddresses;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PollingMasterInquireClient)) {
      return false;
    }
    PollingMasterInquireClient that = (PollingMasterInquireClient) o;
    return mMasterAddresses.equals(that.mMasterAddresses);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mMasterAddresses);
  }
}
