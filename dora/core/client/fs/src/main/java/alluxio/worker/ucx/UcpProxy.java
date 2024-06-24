package alluxio.worker.ucx;

import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;

import java.util.concurrent.locks.ReentrantLock;

public class UcpProxy {

  private static final UcpContext sGlobalContext = new UcpContext(new UcpParams()
      .requestStreamFeature()
      .requestTagFeature()
      .requestWakeupFeature());
  public UcpWorker mWorker;
  public static UcpProxy sUcpProxy;
  private static ReentrantLock sInstanceLock = new ReentrantLock();

  public static UcpProxy getInstance() {
    if (sUcpProxy != null) {
      return sUcpProxy;
    }
    sInstanceLock.lock();
    try {
      if (sUcpProxy != null) {
        return sUcpProxy;
      }
      sUcpProxy = new UcpProxy();
      return sUcpProxy;
    } finally {
      sInstanceLock.unlock();
    }
  }

  public UcpProxy() {
    mWorker = sGlobalContext.newWorker(new UcpWorkerParams());
  }

}
