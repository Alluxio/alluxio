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
  private class NamedOutputStream {
    private final OutputStream mStream;
    private final String mPath;

    private NamedOutputStream(OutputStream stream, String path) {
      mStream = stream;
      mPath = path;
    }

    public OutputStream getStream() {
      return mStream;
    }

    public String getPath() {
      return mPath;
    }
  }


  private ConcurrentMap<Long, NamedOutputStream> mStreams;

  public UnderFileSystemManager() {
    mStreams = new ConcurrentHashMap<>();
  }

  public OutputStream createFile(String path) throws FileAlreadyExistsException, IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path, WorkerContext.getConf());
    if (ufs.exists(path)) {
      throw new FileAlreadyExistsException(ExceptionMessage.FAILED_UFS_CREATE.getMessage(path));
    }

    OutputStream newStream = ufs.create(path);
    stream = mStreams.putIfAbsent(path, newStream);
  }

  public void cancelFile(long workerFileId) throws FileDoesNotExistException, IOException {
    closeFile(path);
    UnderFileSystem ufs = UnderFileSystem.get(path, WorkerContext.getConf());
    ufs.delete(path, false);
  }

  public void completeFile(long workerFileId) throws FileDoesNotExistException, IOException {
    closeFile(path);
  }

  // TODO(calvin): Make the exception accurate.
  private void closeFile(long workerFileId) throws FileDoesNotExistException, IOException {
    OutputStream stream = mStreams.remove(workerFileId);
    if (stream != null) {
      stream.close();
    } else {
      throw
          new FileDoesNotExistException(ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(path));
    }
  }
}
