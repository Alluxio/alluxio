package alluxio.client.file.dora.event;

import alluxio.client.file.dora.PartialReadException;

public class TransportErrorResponseEvent implements ResponseEvent {

  private final Throwable mCause;

  public TransportErrorResponseEvent(Throwable cause) {
    mCause = cause;
  }

  @Override
  public void postProcess(ResponseEventContext responseEventContext) throws PartialReadException {
    throw new PartialReadException(
        responseEventContext.getLengthToRead(),
        responseEventContext.getBytesRead(),
        PartialReadException.CauseType.TRANSPORT_ERROR,
        mCause);
  }

}
