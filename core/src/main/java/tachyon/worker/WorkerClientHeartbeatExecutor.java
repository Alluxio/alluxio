package tachyon.worker;

import java.io.IOException;

import com.google.common.base.Throwables;

import tachyon.HeartbeatExecutor;

/**
 * User client sends periodical heartbeats to the worker it is talking to. It is fails to do so,
 * the worker may withdraw the space granted to the particular user.
 */
class WorkerClientHeartbeatExecutor implements HeartbeatExecutor {
  private final WorkerClient mWorkerClient;
  private final long mUserId;

  public WorkerClientHeartbeatExecutor(WorkerClient workerClient, long userId) {
    mWorkerClient = workerClient;
    mUserId = userId;
  }

  @Override
  public void heartbeat() {
    try {
      mWorkerClient.userHeartbeat(mUserId);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}