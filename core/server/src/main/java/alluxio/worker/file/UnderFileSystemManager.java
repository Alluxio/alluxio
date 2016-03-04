package alluxio.worker.file;

import alluxio.AlluxioURI;
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
 * Manages all open output streams to an Under File System. Individual streams should only be used
 * by one client.
 *
 * TODO(calvin): Enforce only one writer to each stream at a time
 */
public class UnderFileSystemManager {
  private static ConcurrentMap<AlluxioURI, UnderFileSystemManager> sUriToStreamManagerMap =
      new ConcurrentHashMap<>();

  private UnderFileSystem mUnderFileSystem;
  private ConcurrentMap<String, OutputStream> mFileToOutputStreamMap;

  public static UnderFileSystemManager get(AlluxioURI uri) {
    UnderFileSystemManager mgr = sUriToStreamManagerMap.get(uri);
    if (mgr == null) {
      UnderFileSystemManager newMgr = new UnderFileSystemManager(uri);
      mgr = sUriToStreamManagerMap.putIfAbsent(uri, newMgr);
      if (mgr == null) {
        mgr = newMgr;
      }
    }
    return mgr;
  }

  private UnderFileSystemManager(AlluxioURI uri) {
    mUnderFileSystem = UnderFileSystem.get(uri.toString(), WorkerContext.getConf());
    mFileToOutputStreamMap = new ConcurrentHashMap<>();
  }

  public OutputStream createFile(String path) throws FileAlreadyExistsException, IOException {
    OutputStream stream = mFileToOutputStreamMap.get(path);
    if (stream == null) {
      OutputStream newStream = mUnderFileSystem.create(path);
      stream = mFileToOutputStreamMap.putIfAbsent(path, newStream);
      if (stream == null) {
        return newStream;
      }
    }
    throw new FileAlreadyExistsException(ExceptionMessage.FAILED_UFS_CREATE.getMessage(path));
  }

  public void cancelFile(String path) throws FileDoesNotExistException, IOException {
    closeFile(path);
    mUnderFileSystem.delete(path, false);
  }

  public void closeFile(String path) throws FileDoesNotExistException, IOException {
    OutputStream stream = mFileToOutputStreamMap.remove(path);
    if (stream != null) {
      stream.close();
    } else {
      throw
          new FileDoesNotExistException(ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(path));
    }
  }
}
