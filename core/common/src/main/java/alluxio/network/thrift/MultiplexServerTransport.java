package alluxio.network.thrift;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

public class MultiplexServerTransport extends MultiplexTransport {
  private static final Logger LOG = LoggerFactory.getLogger(MultiplexServerTransport.class);

  private TTransportFactory mTransportFactory;

  public MultiplexServerTransport(TTransport baseTransport, TTransportFactory tf) {
    super(baseTransport);
    mTransportFactory = tf;
  }

  @Override
  public void open() throws TTransportException {
    LOG.debug("opening server transport");
    if (!mUnderlyingTransport.isOpen()) {
      mUnderlyingTransport.open();
    }
    byte[] messageHeader = new byte[TYPE_BYTES];
    try {
      mUnderlyingTransport.read(messageHeader, 0, TYPE_BYTES);
    } catch (TTransportException e) {
      if (e.getType() == TTransportException.END_OF_FILE) {
        mUnderlyingTransport.close();
        LOG.debug("No data in the stream");
        throw new TTransportException("No data data in the stream");
      }
      throw e;
    }
    byte typeByte = messageHeader[0];
    TransportType type = TransportType.byValue(typeByte);
    if (type == null) {
      throw new TTransportException("Invalid transport type " + typeByte);
    } else if (type == TransportType.BOOTSTRAP) {
      mTransport = mUnderlyingTransport;
    } else if (type == TransportType.FINAL) {
      mTransport = mTransportFactory.getTransport(mUnderlyingTransport);
    }
    if (!mTransport.isOpen()) {
      mTransport.open();
    }
  }

  /**
   * Factory to create <code>MultiplexServerTransport</code> instance on server side.
   */
  public static class Factory extends TTransportFactory {
    /**
     * The map to keep the <code>MultiplexServerTransport</code> and ensure the same base transport
     * instance receives the same <code>MultiplexServerTransport</code>. <code>WeakHashMap</code> is
     * used to ensure that we don't leak memory.
     */
    private static Map<TTransport, WeakReference<MultiplexServerTransport>> transportMap =
        Collections
            .synchronizedMap(new WeakHashMap<TTransport, WeakReference<MultiplexServerTransport>>());
    private TTransportFactory mTransportFactory;

    public Factory(TTransportFactory tf) {
      mTransportFactory = tf;
    }

    @Override
    public TTransport getTransport(TTransport base) {
      LOG.debug("Transport Factory getTransport: {}", base);
      WeakReference<MultiplexServerTransport> ret = transportMap.get(base);
      if (ret == null || ret.get() == null) {
        MultiplexServerTransport transport = new MultiplexServerTransport(base, mTransportFactory);
        ret = new WeakReference<>(transport);
        try {
          transport.open();
        } catch (TTransportException e) {
          LOG.debug("failed to open server transport", e);
          throw new RuntimeException(e);
        }
        transportMap.put(base, ret);
      }
      return ret.get();
    }
  }
}
