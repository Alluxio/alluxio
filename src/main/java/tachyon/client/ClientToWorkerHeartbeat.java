package tachyon.client;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.CommonUtils;
import tachyon.WorkerClient;
import tachyon.conf.CommonConf;
import tachyon.conf.UserConf;

class ClientToWorkerHeartbeat implements Runnable {
  private final Logger LOG = Logger.getLogger(CommonConf.get().LOGGER_TYPE);
  private final WorkerClient WORKER_CLIENT;
  private final long USER_ID;
  private final long HEARTBEAT_INTERVAL_MS;

  public ClientToWorkerHeartbeat(WorkerClient workerClient, long userId) {
    WORKER_CLIENT = workerClient;
    USER_ID = userId;
    HEARTBEAT_INTERVAL_MS = UserConf.get().HEARTBEAT_INTERVAL_MS;
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

      CommonUtils.sleepMs(LOG, HEARTBEAT_INTERVAL_MS);
    }
  }
}