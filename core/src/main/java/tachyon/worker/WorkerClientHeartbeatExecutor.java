package tachyon.worker;

import java.io.IOException;

import com.google.common.base.Throwables;

import tachyon.HeartbeatExecutor;

/**
 * User client sends periodical heartbeats to the worker it is talking to. It is fails to do so,
 * the worker may withdraw the space granted to the particular user.
 */
class WorkerClientHeartbeatExecutor implements HeartbeatExecutor {
  private final WorkerClient WORKER_CLIENT;
  private final long USER_ID;

  public WorkerClientHeartbeatExecutor(WorkerClient workerClient, long userId) {
    WORKER_CLIENT = workerClient;
    USER_ID = userId;
  }

  @Override
  public void heartbeat() {
    try {
      WORKER_CLIENT.userHeartbeat(USER_ID);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}