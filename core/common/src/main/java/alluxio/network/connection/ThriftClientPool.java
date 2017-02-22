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

package alluxio.network.connection;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.exception.ExceptionMessage;
import alluxio.resource.DynamicResourcePool;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.authentication.TransportProvider;
import alluxio.thrift.AlluxioService;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.regex.Pattern;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * A pool to manage Alluxio thrift clients.
 * 1. It is recommended to keep one ThriftClientPool instance per <serverAddress, serviceType> pair.
 * 2. Make sure to release every client acquired from the pool even when the client is disconnected
 *    An example usage:
 *    ClientType client = pool.acquire();
 *    try {
 *      client.doRpc();
 *    } catch (TTransportException e) {
 *      client.getOutputProtocol().getTransport().close();
 *    } finally {
 *      pool.release(client)
 *    }
 *
 * @param <T> the Alluxio thrift service type
 */
// TODO(peis): Add unittest.
@ThreadSafe
public abstract class ThriftClientPool<T extends AlluxioService.Client>
    extends DynamicResourcePool<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftClientPool.class);

  private final TransportProvider mTransportProvider;
  private final String mServiceName;
  private final long mServiceVersion;
  private final InetSocketAddress mAddress;
  private final long mGcThresholdMs;
  private final Subject mParentSubject;

  private static final int THRIFT_CLIENT_POOL_GC_THREADPOOL_SIZE = 5;
  private static final ScheduledExecutorService GC_EXECUTOR =
      new ScheduledThreadPoolExecutor(THRIFT_CLIENT_POOL_GC_THREADPOOL_SIZE,
          ThreadFactoryUtils.build("ThriftClientPoolGcThreads-%d", true));

  @GuardedBy("this")
  private Long mServerVersionFound = null;

  private static final int BASE_SLEEP_MS =
      Configuration.getInt(PropertyKey.USER_RPC_RETRY_BASE_SLEEP_MS);
  private static final int MAX_SLEEP_MS =
      Configuration.getInt(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS);
  private static final int RPC_MAX_NUM_RETRY =
      Configuration.getInt(PropertyKey.USER_RPC_RETRY_MAX_NUM_RETRY);

  /**
   * The patterns of exception message when client and server transport frame sizes do not match
   * or corrupted data (e.g. due to incorrect port configured).
   */
  private static final Pattern FRAME_SIZE_TOO_LARGE_EXCEPTION_PATTERN =
      Pattern.compile("Frame size \\((\\d+)\\) larger than max length");
  private static final Pattern FRAME_SIZE_NEGATIVE_EXCEPTION_PATTERN =
      Pattern.compile("Read a negative frame size");

  /**
   * Creates a thrift client pool instance with a minimum capacity of 1.
   *
   * @param subject the parent subject, set to null if not present
   * @param serviceName the service name (e.g. BlockWorkerClient)
   * @param serviceVersion the service version
   * @param address the server address
   * @param maxCapacity the maximum capacity of the pool
   * @param gcThresholdMs when a channel is older than this threshold and the pool's capacity
   *        is above the minimum capacity (1), it is closed and removed from the pool.
   */
  public ThriftClientPool(Subject subject, String serviceName, long serviceVersion,
      InetSocketAddress address, int maxCapacity, long gcThresholdMs) {
    super(Options.defaultOptions().setMaxCapacity(maxCapacity).setGcExecutor(GC_EXECUTOR));
    mTransportProvider = TransportProvider.Factory.create();
    mServiceName = serviceName;
    mServiceVersion = serviceVersion;
    mAddress = address;
    mGcThresholdMs = gcThresholdMs;
    mParentSubject = subject;
  }

  /**
   * A helper function to close thrift clients.
   *
   * @param client the thrift client to close
   * @param <C> the thrift client type
   */
  public static <C extends AlluxioService.Client> void closeThriftClient(C client) {
    // Note that the input and output protocol is the same in Alluxio.
    TTransport transport = client.getOutputProtocol().getTransport();
    if (transport.isOpen()) {
      transport.close();
    }
  }

  @Override
  protected void closeResource(T client) {
    closeThriftClient(client);
  }

  @Override
  protected void closeResourceSync(T client) {
    closeResource(client);
  }

  /**
   * Creates a thrift client instance.
   *
   * @return the thrift client created
   * @throws IOException if it fails to create a thrift client
   */
  @Override
  protected T createNewResource() throws IOException {
    TTransport transport = mTransportProvider.getClientTransport(mParentSubject, mAddress);
    TProtocol binaryProtocol = new TBinaryProtocol(transport);
    T client = createThriftClient(new TMultiplexedProtocol(binaryProtocol, mServiceName));

    TException exception;
    RetryPolicy retryPolicy =
        new ExponentialBackoffRetry(BASE_SLEEP_MS, MAX_SLEEP_MS, RPC_MAX_NUM_RETRY);
    do {
      LOG.info("Alluxio client (version {}) is trying to connect with {} {} @ {}",
          RuntimeConstants.VERSION, mServiceName, mAddress);
      try {
        if (!transport.isOpen()) {
          transport.open();
        }
        if (transport.isOpen()) {
          checkVersion(client);
        }
        LOG.info("Client registered with {} @ {}", mServiceName, mAddress);
        return client;
      } catch (TTransportException e) {
        if (e.getCause() instanceof java.net.SocketTimeoutException) {
          // Do not retry if socket timeout.
          String message = "Thrift transport open times out. Please check whether the "
              + "authentication types match between client and server. Note that NOSASL client "
              + "is not able to connect to servers with SIMPLE security mode.";
          throw new IOException(message, e);
        }
        LOG.warn("Failed to connect ({}) to {} @ {}: {}", retryPolicy.getRetryCount(), mServiceName,
            mAddress, e.getMessage());
        exception = e;
      }
    } while (retryPolicy.attemptRetry());

    LOG.error("Failed after " + retryPolicy.getRetryCount() + " retries.");
    Preconditions.checkNotNull(exception);
    throw new IOException(exception);
  }

  /**
   * Checks whether a client is healthy.
   *
   * @param client the thrift client to check
   * @return true if the client is open (i.e. connected)
   */
  @Override
  protected boolean isHealthy(T client) {
    return client.getOutputProtocol().getTransport().isOpen();
  }

  @Override
  protected boolean shouldGc(ResourceInternal<T> clientResourceInternal) {
    return System.currentTimeMillis() - clientResourceInternal.getLastAccessTimeMs()
        > mGcThresholdMs;
  }

  /**
   * Check the service version to see whether it matches the expected version.
   *
   * @param client the client
   * @throws IOException if it fails to check version
   */
  private void checkVersion(T client) throws TTransportException, IOException {
    synchronized (this) {
      if (mServerVersionFound != null) {
        if (mServerVersionFound != mServiceVersion) {
          throw new IOException(ExceptionMessage.INCOMPATIBLE_VERSION
              .getMessage(mServiceName, mServiceVersion, mServerVersionFound));
        }
        return;
      }
    }

    try {
      long serviceVersionFound = client.getServiceVersion();
      synchronized (this) {
        mServerVersionFound = serviceVersionFound;
        if (mServerVersionFound != mServiceVersion) {
          throw new IOException(ExceptionMessage.INCOMPATIBLE_VERSION
              .getMessage(mServiceName, mServiceVersion, mServerVersionFound));
        }
      }
    } catch (TTransportException e) {
      closeResource(client);
      // The master branch of Apache Thrift provides a dedicated exception type for this
      // (CORRUPTED_DATA).
      if (e.getMessage() != null && (
          FRAME_SIZE_NEGATIVE_EXCEPTION_PATTERN.matcher(e.getMessage()).find()
              || FRAME_SIZE_TOO_LARGE_EXCEPTION_PATTERN.matcher(e.getMessage()).find())) {
        // See an error like "Frame size (67108864) larger than max length (16777216)!",
        // pointing to the helper page.
        String message = String.format("Failed to connect to %s @ %s: %s. " + "This exception "
                + "may be caused by incorrect network configuration. "
                + "Please consult %s for common solutions to address this problem.",
            getServiceNameForLogging(), mAddress, e.getMessage(),
            RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL);
        throw new IOException(message, e);
      }
      throw e;
    } catch (TException e) {
      closeResource(client);
      throw new IOException(e);
    }
  }

  /**
   * Creates a thrift client from a thrift protocol.
   *
   * @param protocol the thrift protocol
   * @return the created thrift client
   */
  protected abstract T createThriftClient(TProtocol protocol);

  /**
   * Sometimes mServiceName passed from the constructor can be misleading for showing messages
   * to the user. The implementation can optionally override this to display a nice name.
   * This function should only be used for logging related functionality.
   *
   * @return the service name for logging
   */
  protected String getServiceNameForLogging() {
    return mServiceName;
  }
}
