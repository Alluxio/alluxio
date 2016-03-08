package alluxio.worker.file;

import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.underfs.UnderFileSystem;
import alluxio.worker.WorkerContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Handles writes to the under file system. Manages open output streams to Under File Systems.
 * Individual streams should only be used by one client. Each instance keeps its own state of
 * file names to open streams.
 */
public class UnderFileSystemManager {
  private ConcurrentMap<String, OutputStream> mFileToOutputStreamMap;

  public UnderFileSystemManager() {
    mFileToOutputStreamMap = new ConcurrentHashMap<>();
  }

  public OutputStream createFile(String path) throws FileAlreadyExistsException, IOException {
    OutputStream stream = mFileToOutputStreamMap.get(path);
    if (stream == null) {
      UnderFileSystem ufs = UnderFileSystem.get(path, WorkerContext.getConf());
      OutputStream newStream = ufs.create(path);
      stream = mFileToOutputStreamMap.putIfAbsent(path, newStream);
      if (stream == null) {
        return newStream;
      }
    }
    throw new FileAlreadyExistsException(ExceptionMessage.FAILED_UFS_CREATE.getMessage(path));
  }

  public void cancelFile(String path) throws FileDoesNotExistException, IOException {
    closeFileInternal(path);
    UnderFileSystem ufs = UnderFileSystem.get(path, WorkerContext.getConf());
    ufs.delete(path, false);
  }

  public void closeFile(String path) throws FileDoesNotExistException, IOException {
    closeFileInternal(path);
  }

  private void closeFileInternal(String path) throws FileDoesNotExistException, IOException {
    OutputStream stream = mFileToOutputStreamMap.remove(path);
    if (stream != null) {
      stream.close();
    } else {
      throw
          new FileDoesNotExistException(ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(path));
    }
  }
}
