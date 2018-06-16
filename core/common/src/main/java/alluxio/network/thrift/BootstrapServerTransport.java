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

import static alluxio.network.thrift.BootstrapClientTransport.BOOTSTRAP_HEADER;

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
public class BootstrapServerTransport extends TTransport {
  private static final Logger LOG = LoggerFactory.getLogger(BootstrapServerTransport.class);

  /** The base transport which we can peek into. */
  private final PeekableTransport mBaseTransport;
  /** The logical transport to work on, can be the base transport or the real transport. */
  private TTransport mTransport;
  /** The factory to create the logical transport on open. */
  private final TTransportFactory mTransportFactory;

  /**
   * @param baseTransport base transport
   * @param tf transport factory to create the fallback transport
   */
  public BootstrapServerTransport(TTransport baseTransport, TTransportFactory tf) {
    mBaseTransport = new PeekableTransport(baseTransport);
    mTransportFactory = tf;
  }

  @Override
  public void open() throws TTransportException {
    LOG.debug("opening server transport");
    if (!mBaseTransport.isOpen()) {
      mBaseTransport.open();
    }
    byte[] messageHeader = new byte[BOOTSTRAP_HEADER.length];
    int bytes;
    try {
      bytes = mBaseTransport.peek(messageHeader, 0, BOOTSTRAP_HEADER.length);
    } catch (TTransportException e) {
      if (e.getType() == TTransportException.END_OF_FILE) {
        LOG.debug("No data in the stream {}", mBaseTransport);
        mBaseTransport.close();
        throw new TTransportException("No data in the stream.");
      }
      throw e;
    }

    if (bytes == BOOTSTRAP_HEADER.length && Arrays.equals(messageHeader, BOOTSTRAP_HEADER)) {
      mBaseTransport.consumeBuffer(BOOTSTRAP_HEADER.length);
      mTransport = mBaseTransport;
    } else {
      mTransport = mTransportFactory.getTransport(mBaseTransport);
    }
    if (!mTransport.isOpen()) {
      mTransport.open();
    }
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
      throw new TTransportException("transport is not open: " + mTransport);
    }
    return mTransport.read(buf, off, len);
  }

  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    if (!isOpen()) {
      throw new TTransportException("transport is not open: " + mTransport);
    }
    mTransport.write(buf, off, len);
  }

  @Override
  public void flush() throws TTransportException {
    if (!isOpen()) {
      throw new TTransportException("transport is not open: " + mTransport);
    }
    mTransport.flush();
  }

  /**
   * @return the underlying transport
   */
  public TTransport getBaseTransport() {
    return mBaseTransport.getBaseTransport();
  }

  /**
   * Factory to create <code>BootstrapServerTransport</code> instance on server side.
   */
  public static class Factory extends TTransportFactory {
    /**
     * The map to keep the BootstrapServerTransport and ensure the same base transport
     * instance receives the same BootstrapServerTransport. <code>WeakHashMap</code> is
     * used to ensure that we don't leak memory. This workaround is inspired by the
     * implementation of <code>org.apache.thrift.transport.TSaslServerTransport</code>.
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
