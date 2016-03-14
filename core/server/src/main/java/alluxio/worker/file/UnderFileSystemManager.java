package alluxio.worker.file;

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
  private class TemporaryOutputStream {
    private final OutputStream mStream;
    private final String mPath;
    private final String mTemporaryPath;

    private TemporaryOutputStream(OutputStream stream, String path, String temporaryPath) {
      long seed = Math.abs(new Random().nextLong());
      mStream = stream;
      mPath = path;
      mTemporaryPath = temporaryPath;
    }

    public OutputStream getStream() {
      return mStream;
    }

    public String getPath() {
      return mPath;
    }

    public String getTemporaryPath() {
      return mTemporaryPath;
    }
  }

  private AtomicLong mIdGenerator;
  private ConcurrentMap<Long, TemporaryOutputStream> mStreams;

  public UnderFileSystemManager() {
    mStreams = new ConcurrentHashMap<>();
  }

  public long createFile(String ufsPath) throws FileAlreadyExistsException, IOException {
    UnderFileSystem ufs = UnderFileSystem.get(ufsPath, WorkerContext.getConf());
    if (ufs.exists(ufsPath)) {
      throw new FileAlreadyExistsException(ExceptionMessage.FAILED_UFS_CREATE.getMessage(ufsPath));
    }
    long workerFileId = mIdGenerator.getAndIncrement();
    String tempPath = PathUtils.temporaryFileName(workerFileId, ufsPath);
    OutputStream os = ufs.create(tempPath);
    TemporaryOutputStream stream = new TemporaryOutputStream(os, ufsPath, tempPath);
    mStreams.put(workerFileId, stream);
    return workerFileId;
  }

  // TODO(calvin): Add the exception to Exception Messages
  public void cancelFile(long workerFileId) throws FileDoesNotExistException, IOException {
    TemporaryOutputStream stream = mStreams.remove(workerFileId);
    if (stream != null) {
      stream.getStream().close();
    } else {
      throw new FileDoesNotExistException("Bad worker file id: " + workerFileId);
    }
    UnderFileSystem ufs = UnderFileSystem.get(stream.getPath(), WorkerContext.getConf());
    ufs.delete(stream.getPath(), false);
  }

  // TODO(calvin): Add the exception to Exception Messages
  public void completeFile(long workerFileId) throws FileDoesNotExistException, IOException {
    TemporaryOutputStream stream = mStreams.remove(workerFileId);
    if (stream != null) {
      stream.getStream().close();
    } else {
      throw new FileDoesNotExistException("Bad worker file id: " + workerFileId);
    }
  }
}
