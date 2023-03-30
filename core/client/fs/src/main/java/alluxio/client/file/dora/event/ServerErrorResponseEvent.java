package alluxio.client.file.dora.event;

import alluxio.client.file.dora.PartialReadException;
import alluxio.exception.status.AlluxioStatusException;

public class ServerErrorResponseEvent implements ResponseEvent {

  private final AlluxioStatusException mAlluxioStatusException;

  public ServerErrorResponseEvent(AlluxioStatusException alluxioStatusException) {
    mAlluxioStatusException = alluxioStatusException;
  }

  @Override
  public void postProcess(ResponseEventContext responseEventContext) throws PartialReadException {
    throw new PartialReadException(
        responseEventContext.getLengthToRead(),
        responseEventContext.getBytesRead(),
        PartialReadException.CauseType.SERVER_ERROR,
        mAlluxioStatusException);
  }

}
