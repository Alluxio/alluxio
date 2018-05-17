package alluxio.network.thrift;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class SwitchingClientTransport extends SwitchingTransport {
  private static final Logger LOG = LoggerFactory.getLogger(SwitchingClientTransport.class);
  private Supplier<TTransport> mTransportSupplier;

  public SwitchingClientTransport(TTransport baseTransport, Supplier<TTransport> supplier) {
    super(baseTransport);
    mTransportSupplier = supplier;
  }

  @Override
  public void open() throws TTransportException {
    open(TransportType.NORMAL);
  }

  public void open(TransportType type) throws TTransportException {
    LOG.error("opening bootstrap client transport {}", this);
    if (!mUnderlyingTransport.isOpen()) {
      mUnderlyingTransport.open();
    }
    //sendHeader(type);
    if (type == TransportType.BOOTSTRAP) {
      mTransport = mUnderlyingTransport;
    } else {
      mTransport = mTransportSupplier.get();
    }
    if (!mTransport.isOpen()) {
      mTransport.open();
    }
  }

  private void sendHeader(TransportType type) throws TTransportException {
    LOG.error("opening bootstrap client transport {}", this);
    byte[] header = new byte[TYPE_BYTES];
    header[0] = type.getValue();
    mUnderlyingTransport.write(header, 0, TYPE_BYTES);
    mUnderlyingTransport.flush();
  }
}
