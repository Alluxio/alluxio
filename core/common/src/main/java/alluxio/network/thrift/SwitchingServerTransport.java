package alluxio.network.thrift;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SwitchingServerTransport extends SwitchingTransport {
  private static final Logger LOG = LoggerFactory.getLogger(SwitchingServerTransport.class);

  private TTransportFactory mTransportFactory;

  public SwitchingServerTransport(TTransport baseTransport, TTransportFactory tf) {
    super(baseTransport);
    mTransportFactory = tf;
  }

  @Override
  public void open() throws TTransportException {
    LOG.info("opening server transport {}", this);
    if (!mUnderlyingTransport.isOpen()) {
      mUnderlyingTransport.open();
    }
//    byte[] messageHeader = new byte[TYPE_BYTES];
//    try {
//      mUnderlyingTransport.read(messageHeader, 0, TYPE_BYTES);
//    } catch (TTransportException e) {
//      if (e.getType() == TTransportException.END_OF_FILE) {
//        mUnderlyingTransport.close();
//        LOG.debug("No data in the stream");
//        throw new TTransportException("No data data in the stream");
//      }
//      throw e;
//    }
//    byte typeByte = messageHeader[0];
    byte typeByte = 1;
    TransportType type = TransportType.byValue(typeByte);
    LOG.info("receive type {}", type);
    if (type == null) {
      throw new TTransportException("Invalid transport type " + typeByte);
    } else if (type == TransportType.BOOTSTRAP) {
      mTransport = mUnderlyingTransport;
      LOG.info("Open bootstrap transport");
    } else if (type == TransportType.NORMAL) {
      mTransport = mTransportFactory.getTransport(mUnderlyingTransport);
      LOG.info("Open normal transport");
    }
    super.open();
  }

  public static class Factory extends TTransportFactory {
    private TTransportFactory mTransportFactory;

    public Factory(TTransportFactory tf) {
      mTransportFactory = tf;
    }

    @Override
    public TTransport getTransport(TTransport base) {
      LOG.info("getTransport: {}", base);
      return new SwitchingServerTransport(base, mTransportFactory);
    }
  }
}
