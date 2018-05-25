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
 * A transport that is the wrapper of two different types of transports.
 */
public abstract class BootstrapTransport extends TTransport {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapTransport.class);

  /** The base transport underlying which we can peek into. */
  protected PeekableTransport mUnderlyingTransport;
  /** The logic transport to work on, can be base transport or the real transport. */
  protected TTransport mTransport;

  protected static final int BOOTSTRAP_HEADER_LENGTH = 8;
  protected static final byte[] BOOTSTRAP_HEADER = new byte[] {127, -128, 34, 12, -120, 22, -37,
      85};

  public BootstrapTransport(TTransport baseTransport) {
    mUnderlyingTransport = new PeekableTransport(baseTransport);
  }

  @Override
  public boolean isOpen() {
    return mUnderlyingTransport.isOpen() && mTransport != null && mTransport.isOpen();
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


}
