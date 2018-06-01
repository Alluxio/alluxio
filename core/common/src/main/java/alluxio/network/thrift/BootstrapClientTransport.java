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

package alluxio.network.thrift;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The client side transport of a BootstrapTransport.
 */
public class BootstrapClientTransport extends TTransport {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapClientTransport.class);

  /** The magic number to look for in message header to indicate this is a bootstrap rpc. */
  static final byte[] BOOTSTRAP_HEADER = new byte[] {127, -128, 34, 12, -120, 22, -37, 85};

  /** The base transport. */
  private final TTransport mTransport;

  /**
   * Constructs a client-side transport of bootstrap transport.
   *
   * @param baseTransport the base transport
   */
  public BootstrapClientTransport(TTransport baseTransport) {
    mTransport = baseTransport;
  }

  @Override
  public void open() throws TTransportException {
    LOG.debug("opening client transport {}", this);
    if (!mTransport.isOpen()) {
      mTransport.open();
    }
    sendHeader();
  }

  @Override
  public boolean isOpen() {
    return mTransport != null && mTransport.isOpen();
  }

  @Override
  public void close() {
    if (isOpen()) {
      mTransport.close();
    }
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    if (!isOpen()) {
      throw new TTransportException("transport is not open");
    }
    return mTransport.read(buf, off, len);
  }

  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    if (!isOpen()) {
      throw new TTransportException("transport is not open");
    }
    mTransport.write(buf, off, len);
  }

  @Override
  public void flush() throws TTransportException {
    if (!isOpen()) {
      throw new TTransportException("transport is not open");
    }
    mTransport.flush();
  }

  private void sendHeader() throws TTransportException {
    mTransport.write(BOOTSTRAP_HEADER, 0, BOOTSTRAP_HEADER.length);
    mTransport.flush();
  }
}
