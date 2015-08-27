package tachyon.client.next.file;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import tachyon.client.FileSystemMasterClient;
import tachyon.client.next.ResourcePool;
import tachyon.conf.TachyonConf;
import tachyon.util.ThreadFactoryUtils;

public class FSMasterClientPool extends ResourcePool<FileSystemMasterClient> {

  private final ExecutorService mExecutorService;
  private final InetSocketAddress mMasterAddress;
  private final TachyonConf mTachyonConf;

  public FSMasterClientPool(InetSocketAddress masterAddress, TachyonConf conf) {
    // TODO: Get capacity from conf
    super(10);
    mExecutorService =
        Executors.newFixedThreadPool(10, ThreadFactoryUtils.build("fs-master-heartbeat-%d", true));
    mMasterAddress = masterAddress;
    mTachyonConf = conf;
  }

  @Override
  public void close() {
    // TODO: Consider collecting all the clients and shutting them down
    mExecutorService.shutdown();
  }

  @Override
  public FileSystemMasterClient createNewResource() {
    return new FileSystemMasterClient(mMasterAddress, mExecutorService, mTachyonConf);
  }
}
