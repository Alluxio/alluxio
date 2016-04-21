package alluxio.worker.netty;

import alluxio.Configuration;
import alluxio.worker.file.FileSystemWorker;

/**
 * This class handles filesystem data server requests.
 */
public class FileDataServerHandler {
  /** Filesystem worker which handles file level operations for the worker */
  private final FileSystemWorker mWorker;

  public FileDataServerHandler(FileSystemWorker worker, Configuration conf) {
    mWorker = worker;
  }
}
