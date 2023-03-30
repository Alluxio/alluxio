package alluxio.client.file.dora.event;

import alluxio.client.file.dora.PartialReadException;
import alluxio.exception.status.CancelledException;

public class CancelledResponseEvent implements ResponseEvent {

  private final CancelledException mCancelledException;

  public CancelledResponseEvent(CancelledException cancelledException) {
    mCancelledException = cancelledException;
  }

  @Override
  public void postProcess(ResponseEventContext responseEventContext) throws PartialReadException {
    throw new PartialReadException(responseEventContext.getLengthToRead(),
        responseEventContext.getBytesRead(),
        PartialReadException.CauseType.CANCELLED, mCancelledException);
  }

}
