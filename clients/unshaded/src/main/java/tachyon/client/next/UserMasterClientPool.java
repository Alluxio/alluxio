package tachyon.client.next;

import tachyon.client.UserMasterClient;
import tachyon.conf.TachyonConf;
import tachyon.util.ThreadFactoryUtils;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UserMasterClientPool extends ResourcePool<UserMasterClient> {
  private final ExecutorService mExecutorService;
  private final InetSocketAddress mMasterAddress;
  private final TachyonConf mTachyonConf;

  public UserMasterClientPool(InetSocketAddress masterAddress, TachyonConf conf) {
    // TODO: Get capacity from conf
    super(10);
    mExecutorService = Executors.newFixedThreadPool(10, ThreadFactoryUtils.build(
        "user-master-heartbeat-%d", true));
    mMasterAddress = masterAddress;
    mTachyonConf = conf;
  }

  @Override
  public void close() {
    // TODO: Consider collecting all the clients and shutting them down
    mExecutorService.shutdown();
  }

  @Override
  public UserMasterClient createNewResource() {
    return new UserMasterClient(mMasterAddress, mExecutorService, mTachyonConf);
  }
}
