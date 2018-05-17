package alluxio.network.thrift;

import alluxio.util.ThreadUtils;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

public class SwitchingTransport extends TTransport {
  private static final Logger LOG = LoggerFactory.getLogger(SwitchingTransport.class);

  /** Transport underlying this one. */
  protected TTransport mUnderlyingTransport;
  protected TTransport mTransport;

  protected static final int TYPE_BYTES = 1;

  public enum TransportType {
    BOOTSTRAP((byte) 0x01),
    NORMAL((byte) 0x02);
    private final byte value;
    private static final Map<Byte, TransportType> reverseMap = new HashMap<>();

    static {
      for (TransportType s : TransportType.values()) {
        reverseMap.put(s.getValue(), s);
      }
    }

    TransportType(byte val) {
      this.value = val;
    }

    public byte getValue() {
      return value;
    }

    @Nullable
    public static TransportType byValue(byte val) {
      return reverseMap.get(val);
    }
  }

  public SwitchingTransport(TTransport baseTransport) {
    mUnderlyingTransport = baseTransport;
  }

  @Override
  public boolean isOpen() {
    return mUnderlyingTransport.isOpen() && mTransport != null && mTransport.isOpen();
  }

  /**
   * Opens the underlying transport if it's not already open and then performs
   */
  @Override
  public void open() throws TTransportException {
    LOG.info("transport open tracestack: {}", ThreadUtils.formatStackTrace(Thread.currentThread()));
    if (mTransport != null && mTransport.isOpen()) {
      throw new TTransportException("transport is already open");
    }
    mTransport.open();
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

}
