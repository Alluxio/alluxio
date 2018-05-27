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
 * The client side transport of <code>BootstrapTransport</code>.
 */
public class BootstrapClientTransport extends BootstrapTransport {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapClientTransport.class);

  /**
   * Constructs a client-side transport of bootstrap transport.
   *
   * @param baseTransport the base transport
   */
  public BootstrapClientTransport(TTransport baseTransport) {
    super(baseTransport);
    mTransport = mUnderlyingTransport;
  }

  @Override
  public void open() throws TTransportException {
    LOG.debug("opening client transport {}", this);
    if (!mUnderlyingTransport.isOpen()) {
      mUnderlyingTransport.open();
    }
    sendHeader();
  }

  private void sendHeader() throws TTransportException {
    mUnderlyingTransport.write(BOOTSTRAP_HEADER, 0, BOOTSTRAP_HEADER_LENGTH);
    mUnderlyingTransport.flush();
  }
}
