package alluxio.client.file.dora.event;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.CancelledException;
import alluxio.network.protocol.databuffer.DataBuffer;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

public class ResponseEventFactory {

  private volatile static ResponseEventFactory responseEventFactory = null;

  private ResponseEventFactory() {
  }

  public static ResponseEventFactory getResponseEventFactory() {
    if (null == responseEventFactory) {
      synchronized (ResponseEventFactory.class) {
        if (null == responseEventFactory) {
          responseEventFactory = new ResponseEventFactory();
        }
      }
    }
    return responseEventFactory;
  }

  public CancelledResponseEvent createCancelResponseEvent(CancelledException cancelledException) {
    return new CancelledResponseEvent(cancelledException);
  }

  public DataResponseEvent createDataResponseEvent(DataBuffer dataBuffer) {
    Preconditions.checkState(dataBuffer.getNettyOutput() instanceof ByteBuf,
        "dataBuffer.getNettyOutput is not of type ByteBuf");
    ByteBuf data = (ByteBuf) dataBuffer.getNettyOutput();
    // need to retain this buffer so that it won't get recycled before we are able to
    // process it
    // will be released in reader
    return new DataResponseEvent(data.retain());
  }

  public EofResponseEvent createEofResponseEvent() {
    return new EofResponseEvent();
  }

  public UfsReadHeartBeatResponseEvent createUfsReadHeartBeatResponseEvent () {
    return new UfsReadHeartBeatResponseEvent();
  }

  public ServerErrorResponseEvent createServerErrorResponseEvent(
      AlluxioStatusException alluxioStatusException) {
    return new ServerErrorResponseEvent(alluxioStatusException);
  }

  public TransportErrorResponseEvent createTransportResponseEvent(
      Throwable cause) {
    return new TransportErrorResponseEvent(cause);
  }

}
