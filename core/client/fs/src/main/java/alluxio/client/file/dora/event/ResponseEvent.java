package alluxio.client.file.dora.event;

import alluxio.client.file.dora.PartialReadException;

public interface ResponseEvent {

  void postProcess(ResponseEventContext responseEventContext) throws PartialReadException;

}
