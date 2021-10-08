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

import static java.util.stream.Collectors.joining;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.CancelledException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.GetServiceVersionPRequest;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.ServiceType;
import alluxio.grpc.ServiceVersionClientServiceGrpc;
import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;
import alluxio.security.user.UserState;
import alluxio.uri.Authority;
import alluxio.uri.MultiMasterAuthority;
import alluxio.util.ConfigurationUtils;

import com.google.common.collect.Lists;
import io.grpc.StatusRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.annotation.Nullable;

/**
 * PollingMasterInquireClient finds the address of the primary master by polling a list of master
 * addresses to see if their RPC servers are serving. This works because only primary masters serve
 * RPCs.
 */
public class PollingMasterInquireClient implements MasterInquireClient {
  private static final Logger LOG = LoggerFactory.getLogger(PollingMasterInquireClient.class);

  private final MultiMasterConnectDetails mConnectDetails;
  private final Supplier<RetryPolicy> mRetryPolicySupplier;
  private final AlluxioConfiguration mConfiguration;
  private final UserState mUserState;

  /**
   * @param masterAddresses the potential master addresses
   * @param alluxioConf Alluxio configuration
   * @param userState user state
   */
  public PollingMasterInquireClient(List<InetSocketAddress> masterAddresses,
      AlluxioConfiguration alluxioConf,
      UserState userState) {
    this(masterAddresses, () -> RetryUtils.defaultClientRetry(
        alluxioConf.getDuration(PropertyKey.USER_RPC_RETRY_MAX_DURATION),
        alluxioConf.getDuration(PropertyKey.USER_RPC_RETRY_BASE_SLEEP_MS),
        alluxioConf.getDuration(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS)),
        alluxioConf, userState);
  }

  /**
   * @param masterAddresses the potential master addresses
   * @param retryPolicySupplier the retry policy supplier
   * @param alluxioConf Alluxio configuration
   */
  public PollingMasterInquireClient(List<InetSocketAddress> masterAddresses,
      Supplier<RetryPolicy> retryPolicySupplier,
      AlluxioConfiguration alluxioConf) {
    mConnectDetails = new MultiMasterConnectDetails(masterAddresses);
    mRetryPolicySupplier = retryPolicySupplier;
    mConfiguration = alluxioConf;
    mUserState = UserState.Factory.create(mConfiguration);
  }

  /**
   * @param masterAddresses the potential master addresses
   * @param retryPolicySupplier the retry policy supplier
   * @param alluxioConf Alluxio configuration
   * @param userState user state
   */
  public PollingMasterInquireClient(List<InetSocketAddress> masterAddresses,
      Supplier<RetryPolicy> retryPolicySupplier,
      AlluxioConfiguration alluxioConf,
      UserState userState) {
    mConnectDetails = new MultiMasterConnectDetails(masterAddresses);
    mRetryPolicySupplier = retryPolicySupplier;
    mConfiguration = alluxioConf;
    mUserState = userState;
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
        mConnectDetails.getAddresses(), retry.getAttemptCount()));
  }

  @Nullable
  private InetSocketAddress getAddress() {
    // Iterate over the masters and try to connect to each of their RPC ports.
    List<InetSocketAddress> addresses;
    if (mConfiguration.getBoolean(PropertyKey.USER_RPC_SHUFFLE_MASTERS_ENABLED)) {
      addresses =
          Lists.newArrayList(mConnectDetails.getAddresses());
      Collections.shuffle(addresses);
    } else {
      addresses = mConnectDetails.getAddresses();
    }

    for (InetSocketAddress address : addresses) {
      try {
        LOG.debug("Checking whether {} is listening for RPCs", address);
        pingMetaService(address);
        LOG.debug("Successfully connected to {}", address);
        return address;
      } catch (UnavailableException e) {
        LOG.debug("Failed to connect to {}", address);
        continue;
      } catch (DeadlineExceededException e) {
        LOG.debug("Timeout while connecting to {}", address);
        continue;
      } catch (CancelledException e) {
        LOG.debug("Cancelled while connecting to {}", address);
        continue;
      } catch (AlluxioStatusException e) {
        LOG.error("Error while connecting to {}. {}", address, e);
        // Breaking the loop on non filtered error.
        break;
      }
    }
    return null;
  }

  private void pingMetaService(InetSocketAddress address) throws AlluxioStatusException {
    // disable authentication in the channel since version service does not require authentication
    GrpcChannel channel =
        GrpcChannelBuilder.newBuilder(GrpcServerAddress.create(address), mConfiguration)
            .setSubject(mUserState.getSubject()).setClientType("MasterInquireClient")
            .disableAuthentication().build();
    ServiceVersionClientServiceGrpc.ServiceVersionClientServiceBlockingStub versionClient =
        ServiceVersionClientServiceGrpc.newBlockingStub(channel)
            .withDeadlineAfter(mConfiguration.getMs(PropertyKey.USER_MASTER_POLLING_TIMEOUT),
                TimeUnit.MILLISECONDS);
    List<InetSocketAddress> addresses = ConfigurationUtils.getJobMasterRpcAddresses(mConfiguration);
    ServiceType serviceType = addresses.contains(address)
        ? ServiceType.JOB_MASTER_CLIENT_SERVICE : ServiceType.META_MASTER_CLIENT_SERVICE;
    try {
      versionClient.getServiceVersion(GetServiceVersionPRequest.newBuilder()
          .setServiceType(serviceType).build());
    } catch (StatusRuntimeException e) {
      throw AlluxioStatusException.fromThrowable(e);
    } finally {
      channel.shutdown();
    }
  }

  @Override
  public List<InetSocketAddress> getMasterRpcAddresses() {
    return mConnectDetails.getAddresses();
  }

  @Override
  public ConnectDetails getConnectDetails() {
    return mConnectDetails;
  }

  /**
   * Details used to connect to the leader Alluxio master when there are multiple potential leaders.
   */
  public static class MultiMasterConnectDetails implements ConnectDetails {
    private final List<InetSocketAddress> mAddresses;

    /**
     * @param addresses a list of addresses
     */
    public MultiMasterConnectDetails(List<InetSocketAddress> addresses) {
      mAddresses = addresses;
    }

    /**
     * @return the addresses
     */
    public List<InetSocketAddress> getAddresses() {
      return mAddresses;
    }

    @Override
    public Authority toAuthority() {
      return new MultiMasterAuthority(mAddresses.stream()
          .map(addr -> addr.getHostString() + ":" + addr.getPort()).collect(joining(",")));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof MultiMasterConnectDetails)) {
        return false;
      }
      MultiMasterConnectDetails that = (MultiMasterConnectDetails) o;
      return mAddresses.equals(that.mAddresses);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mAddresses);
    }

    @Override
    public String toString() {
      return toAuthority().toString();
    }
  }
}
