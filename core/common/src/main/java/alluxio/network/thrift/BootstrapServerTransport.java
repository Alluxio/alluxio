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
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * The server side of bootstrap transport. This transport can fallback to the real RPC transport
 * if the magic number is not found in the message header.
 */
public class BootstrapServerTransport extends BootstrapTransport {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapServerTransport.class);

  private TTransportFactory mTransportFactory;

  /**
   * @param baseTransport base transport
   * @param tf transport factory to create the fallback transport
   */
  public BootstrapServerTransport(TTransport baseTransport, TTransportFactory tf) {
    super(baseTransport);
    mTransportFactory = tf;
  }

  @Override
  public void open() throws TTransportException {
    LOG.debug("opening server transport");
    if (!mUnderlyingTransport.isOpen()) {
      mUnderlyingTransport.open();
    }
    byte[] messageHeader = new byte[BOOTSTRAP_HEADER_LENGTH];
    int bytes = 0;
    try {
      bytes = mUnderlyingTransport.peek(messageHeader, 0, BOOTSTRAP_HEADER_LENGTH);
    } catch (TTransportException e) {
      if (e.getType() == TTransportException.END_OF_FILE) {
        LOG.debug("No data in the stream {}", mUnderlyingTransport);
        mUnderlyingTransport.close();
        throw new TTransportException("No data data in the stream.");
      }
      throw e;
    }

    if (bytes == BOOTSTRAP_HEADER_LENGTH && Arrays.equals(messageHeader, BOOTSTRAP_HEADER)) {
      mUnderlyingTransport.consumeBuffer(BOOTSTRAP_HEADER_LENGTH);
      mTransport = mUnderlyingTransport;
    } else {
      mTransport = mTransportFactory.getTransport(mUnderlyingTransport);
    }
    if (!mTransport.isOpen()) {
      mTransport.open();
    }
  }

  /**
   * Factory to create <code>BootstrapServerTransport</code> instance on server side.
   */
  public static class Factory extends TTransportFactory {
    /**
     * The map to keep the <code>BootstrapServerTransport</code> and ensure the same base transport
     * instance receives the same <code>MultiplexServerTransport</code>. <code>WeakHashMap</code> is
     * used to ensure that we don't leak memory.
     */
    private static Map<TTransport, WeakReference<BootstrapServerTransport>> sTransportMap =
        Collections.synchronizedMap(
            new WeakHashMap<TTransport, WeakReference<BootstrapServerTransport>>());
    private TTransportFactory mTransportFactory;

    /**
     * @param tf transport factory
     */
    public Factory(TTransportFactory tf) {
      mTransportFactory = tf;
    }

    @Override
    public TTransport getTransport(TTransport base) {
      LOG.debug("Transport Factory getTransport: {}", base);
      WeakReference<BootstrapServerTransport> ret = sTransportMap.get(base);
      if (ret == null || ret.get() == null) {
        BootstrapServerTransport transport = new BootstrapServerTransport(base, mTransportFactory);
        ret = new WeakReference<>(transport);
        try {
          transport.open();
        } catch (TTransportException e) {
          LOG.debug("failed to open server transport", e);
          throw new RuntimeException(e);
        }
        sTransportMap.put(base, ret);
      }
      return ret.get();
    }
  }
}
