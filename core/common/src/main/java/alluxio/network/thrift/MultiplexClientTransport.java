package alluxio.network.thrift;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The client side transport of <code>MultiplexTransport</code>. The type of client transport is
 * decided when constructing the class.
 */
public class MultiplexClientTransport extends MultiplexTransport {
  private static final Logger LOG = LoggerFactory.getLogger(MultiplexClientTransport.class);
  private final TransportType mType;

  /**
   * Constructs a client-side transport of type <code>TransportType.FINAL</code>.
   *
   * @param transport the final transport
   * @param baseTransport the base transport
   */
  public MultiplexClientTransport(TTransport transport, TTransport baseTransport) {
    super(baseTransport);
    mTransport = transport;
    mType = TransportType.FINAL;
  }

  /**
   * Constructs a client-side transport of type <code>TransportType.BOOTSTRAP</code>.
   *
   * @param baseTransport the base transport
   */
  public MultiplexClientTransport(TTransport baseTransport) {
    super(baseTransport);
    mTransport = mUnderlyingTransport;
    mType = TransportType.BOOTSTRAP;
  }

  @Override
  public void open() throws TTransportException {
    LOG.debug("opening client transport {}, type {}", this, mType);
    if (!mUnderlyingTransport.isOpen()) {
      mUnderlyingTransport.open();
    }
    sendHeader(mType);
    if (mType == TransportType.FINAL && !mTransport.isOpen()) {
      mTransport.open();
    }
  }

  private void sendHeader(TransportType type) throws TTransportException {
    byte[] header = new byte[TYPE_BYTES];
    header[0] = type.getValue();
    mUnderlyingTransport.write(header, 0, TYPE_BYTES);
    mUnderlyingTransport.flush();
  }
}
