package alluxio.client.file;


import alluxio.heartbeat.HeartbeatExecutor;

/**
 * Session client that sends periodic heartbeats to the file system worker in order to preserve
 * its state on the worker.
 */
public class FileSystemWorkerClientHeartbeatExecutor implements HeartbeatExecutor {
  private final FileSystemWorkerClient mFileSystemWorkerClient;

  public FileSystemWorkerClientHeartbeatExecutor(FileSystemWorkerClient client) {
    mFileSystemWorkerClient = client;
  }

  @Override
  public void heartbeat() {
    mFileSystemWorkerClient.periodicHeartbeat();
  }

  @Override
  public void close() {
    // Not responsible for cleaning up the client
  }
}
