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

package alluxio.security.authentication;

import alluxio.Configuration;
import alluxio.Constants;

import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.sasl.SaslException;

/**
 * If authentication type is {@link AuthType#NOSASL}, we use this transport provider which simply
 * uses default Thrift {@link TFramedTransport}.
 */
@ThreadSafe
public final class NoSaslTransportProvider implements TransportProvider {
  /** Timeout for socket in ms. */
  private final int mSocketTimeoutMs;
  /** Max frame size of thrift transport in bytes. */
  private final int mThriftFrameSizeMax;

  /**
   * Constructor for transport provider when authentication type is {@link AuthType#NOSASL}.
   */
  public NoSaslTransportProvider() {
    mSocketTimeoutMs = Configuration.getInt(Constants.SECURITY_AUTHENTICATION_SOCKET_TIMEOUT_MS);
    mThriftFrameSizeMax =
        (int) Configuration.getBytes(Constants.NETWORK_THRIFT_FRAME_SIZE_BYTES_MAX);
  }

  @Override
  public TTransport getClientTransport(InetSocketAddress serverAddress) {
    TTransport tTransport =
        TransportProviderUtils.createThriftSocket(serverAddress, mSocketTimeoutMs);
    return new TFramedTransport(tTransport, mThriftFrameSizeMax);
  }

  @Override
  public TTransportFactory getServerTransportFactory() throws SaslException {
    return new TFramedTransport.Factory(mThriftFrameSizeMax);
  }
}
