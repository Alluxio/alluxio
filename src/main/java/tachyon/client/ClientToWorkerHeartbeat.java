package tachyon.client;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.CommonUtils;
import tachyon.Config;
import tachyon.WorkerClient;

class ClientToWorkerHeartbeat implements Runnable {
  private final Logger LOG = LoggerFactory.getLogger(ClientToWorkerHeartbeat.class);
  private final WorkerClient WORKER_CLIENT;
  private final long USER_ID;

  public ClientToWorkerHeartbeat(WorkerClient workerClient, long userId) {
    WORKER_CLIENT = workerClient;
    USER_ID = userId;
  }

  @Override
  public void run() {
    while (true) {
      try {
        WORKER_CLIENT.userHeartbeat(USER_ID);
      } catch (TException e) {
        LOG.error(e.getMessage());
        break;
      }

      CommonUtils.sleep(Config.USER_HEARTBEAT_INTERVAL_MS);
    }
  }
}