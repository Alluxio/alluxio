package alluxio.worker.ucx;

import org.openucx.jucx.ucp.UcpEndpoint;

public interface UcxRequestHandler {
  public void handle(UcxMessage msg, UcxConnection remoteConn);
}
