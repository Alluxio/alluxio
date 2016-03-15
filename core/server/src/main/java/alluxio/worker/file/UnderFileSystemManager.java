package alluxio.worker.file;

import alluxio.Configuration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;
import alluxio.worker.WorkerContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles writes to the under file system. Manages open output streams to Under File Systems.
 * Individual streams should only be used by one client. Each instance keeps its own state of
 * file names to open streams.
 */
public class UnderFileSystemManager {
  /**
   * A wrapper around the output stream to the under file system. This class handles writing the
   * data to a temporary file. When the stream is closed, the temporary file will attempt to be
   * renamed to the final file path. This stream guarantees the temporary file will be cleaned up
   * when close or cancel is called.
   */
  private class UnderFileSystemOutputStream {
    private final Configuration mConf;
    private final OutputStream mStream;
    private final String mPath;
    private final String mTemporaryPath;

    private UnderFileSystemOutputStream(String ufsPath, Configuration conf)
        throws FileAlreadyExistsException, IOException {
      mConf = conf;
      mPath = ufsPath;
      mTemporaryPath = PathUtils.temporaryFileName(Math.abs(new Random().nextLong()), ufsPath);
      UnderFileSystem ufs = UnderFileSystem.get(mPath, mConf);
      if (ufs.exists(ufsPath)) {
        throw new FileAlreadyExistsException(ExceptionMessage.FAILED_UFS_CREATE.getMessage(ufsPath));
      }
      mStream = ufs.create(mTemporaryPath);
    }

    public OutputStream getStream() {
      return mStream;
    }

    public void cancel() throws IOException {
      mStream.close();
      UnderFileSystem ufs = UnderFileSystem.get(mPath, mConf);
      ufs.delete(mTemporaryPath, false);
    }

    public void close() throws IOException {
      mStream.close();
      UnderFileSystem ufs = UnderFileSystem.get(mPath, mConf);
      ufs.rename(mTemporaryPath, mPath);
    }
  }

  private AtomicLong mIdGenerator;
  private ConcurrentMap<Long, UnderFileSystemOutputStream> mStreams;

  public UnderFileSystemManager() {
    mIdGenerator = new AtomicLong(new Random().nextLong());
    mStreams = new ConcurrentHashMap<>();
  }

  public long createFile(String ufsPath) throws FileAlreadyExistsException, IOException {
    long workerFileId = mIdGenerator.getAndIncrement();
    UnderFileSystemOutputStream stream =
        new UnderFileSystemOutputStream(ufsPath, WorkerContext.getConf());
    mStreams.put(workerFileId, stream);
    return workerFileId;
  }

  // TODO(calvin): Add the exception to Exception Messages
  public void cancelFile(long workerFileId) throws FileDoesNotExistException, IOException {
    UnderFileSystemOutputStream stream = mStreams.remove(workerFileId);
    if (stream != null) {
      stream.cancel();
    } else {
      throw new FileDoesNotExistException("Bad worker file id: " + workerFileId);
    }
  }

  // TODO(calvin): Add the exception to Exception Messages
  public void completeFile(long workerFileId) throws FileDoesNotExistException, IOException {
    UnderFileSystemOutputStream stream = mStreams.remove(workerFileId);
    if (stream != null) {
      stream.close();
    } else {
      throw new FileDoesNotExistException("Bad worker file id: " + workerFileId);
    }
  }
}
