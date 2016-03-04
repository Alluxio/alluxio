package alluxio.worker.file;

import alluxio.Constants;
import alluxio.thrift.FileSystemWorkerClientService;
import alluxio.thrift.UFSCancelFileTOptions;
import alluxio.thrift.UFSCompleteFileTOptions;
import alluxio.thrift.UFSCreateFileTOptions;

public final class FileSystemWorkerClientServiceHandler
    implements FileSystemWorkerClientService.Iface {

  /** File System Worker that carries out most of the operations. */
  private final FileSystemWorker mWorker;

  public FileSystemWorkerClientServiceHandler(FileSystemWorker worker) {
    mWorker = worker;
  }

  @Override
  public long getServiceVersion() {
    return Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_VERSION;
  }

  @Override
  public void ufsCancelFile(String path, UFSCancelFileTOptions options) {

  }

  @Override
  public void ufsCompleteFile(String path, UFSCompleteFileTOptions options) {

  }

  @Override
  public void ufsCreateFile(String path, UFSCreateFileTOptions options) {

  }
}
