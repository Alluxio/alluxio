/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.file;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.collections.IndexedSet;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;
import alluxio.worker.WorkerContext;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Handles writes to the under file system. Manages open output streams to Under File Systems.
 * Individual streams should only be used by one client. Each instance keeps its own state of
 * temporary file ids to open streams.
 */
// TODO(calvin): Consider whether the manager or the agents should contain the execution logic
@ThreadSafe
public final class UnderFileSystemManager {
  /**
   * An object which generates input streams to under file system files given a position. This
   * class does not manage the life cycles of the generated streams and it is up to the caller to
   * close the streams when appropriate.
   */
  @ThreadSafe
  private final class InputStreamAgent {
    /** Session this agent belongs to, indexed on */
    private final long mSessionId;
    /** Id referencing this agent, indexed on */
    private final long mId;
    /** Configuration to use for this stream. */
    private final Configuration mConfiguration;
    /** The string form of the uri to the file in the under file system. */
    private final String mUri;

    /**
     * Constructor for an input stream agent for a UFS file. The file must exist when this is
     * called.
     *
     * @param ufsUri the uri of the file in the UFS
     * @param conf the configuration to use
     * @throws FileDoesNotExistException if the file does not exist in the UFS
     * @throws IOException if an error occurs when interacting with the UFS
     */
    private InputStreamAgent(long sessionId, long id, AlluxioURI ufsUri, Configuration conf)
        throws FileDoesNotExistException, IOException {
      mSessionId = sessionId;
      mId = id;
      mUri = ufsUri.toString();
      mConfiguration = conf;
      UnderFileSystem ufs = UnderFileSystem.get(mUri, mConfiguration);
      if (!ufs.exists(mUri)) {
        throw new FileDoesNotExistException(
            ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(mUri));
      }
    }

    /**
     * Opens a new input stream to the file this agent references. The new stream will be at the
     * specified position when it is returned to the caller. The caller is responsible for
     * closing the stream.
     *
     * @param position the absolute position in the file to start the stream at
     * @return an input stream to the file starting at the specified position
     * @throws IOException if an error occurs when interacting with the UFS
     */
    private InputStream openAtPosition(long position) throws IOException {
      UnderFileSystem ufs = UnderFileSystem.get(mUri, mConfiguration);
      InputStream stream = ufs.open(mUri);
      if (position != stream.skip(position)) {
        throw new IOException(ExceptionMessage.FAILED_SKIP.getMessage(position));
      }
      return stream;
    }
  }

  /**
   * A wrapper around the output stream to the under file system. The output stream provided will be
   * to a temporary file in the UFS. When the stream is closed, the temporary file will attempt to
   * be renamed to the final file path. This agent guarantees the temporary file will be cleaned up
   * when complete or cancel is called. The agent should only be used by one thread at a time.
   *
   * Note that the output stream returned by this agent is expected to live throughout the UFS file
   * write, whereas the input stream agent's input streams may be opened and closed each read call.
   * This is because the UFS may not allow for random writes or reopening a closed output stream, so
   * the output stream cannot be closed with each write.
   */
  // TODO(calvin): This can be defined by the UnderFileSystem
  @NotThreadSafe
  private final class OutputStreamAgent {
    /** Session this agent belongs to, indexed on */
    private final long mSessionId;
    /** Id referencing this agent, indexed on */
    private final long mId;
    /** Configuration to use for this stream. */
    private final Configuration mConfiguration;
    /** Underlying stream to the under file system file. */
    private final OutputStream mStream;
    /** String form of the final uri to write to in the under file system. */
    private final String mUri;
    /** String form of the temporary uri to write to in the under file system. */
    private final String mTemporaryUri;

    /**
     * Creates an output stream agent for the specified UFS uri. The UFS file must not exist when
     * this constructor is called.
     *
     * @param sessionId the id of the session that created this object
     * @param id the worker specific id which references this object
     * @param ufsUri the file to create in the UFS
     * @param conf the configuration to use
     * @throws FileAlreadyExistsException if a file already exists at the uri specified
     * @throws IOException if an error occurs when interacting with the UFS
     */
    private OutputStreamAgent(long sessionId, long id, AlluxioURI ufsUri, Configuration conf)
        throws FileAlreadyExistsException, IOException {
      mSessionId = sessionId;
      mId = id;
      mConfiguration = Preconditions.checkNotNull(conf);
      mUri = Preconditions.checkNotNull(ufsUri).toString();
      mTemporaryUri = PathUtils.temporaryFileName(IdUtils.getRandomNonNegativeLong(), mUri);
      UnderFileSystem ufs = UnderFileSystem.get(mUri, mConfiguration);
      if (ufs.exists(mUri)) {
        throw new FileAlreadyExistsException(ExceptionMessage.FAILED_UFS_CREATE.getMessage(mUri));
      }
      mStream = ufs.create(mTemporaryUri);
    }

    /**
     * Closes the temporary file and deletes it. This object should be discarded after this call.
     *
     * @throws IOException if an error occurs during the under file system operation
     */
    private void cancel() throws IOException {
      mStream.close();
      UnderFileSystem ufs = UnderFileSystem.get(mUri, mConfiguration);
      // TODO(calvin): Log a warning if the delete fails
      ufs.delete(mTemporaryUri, false);
    }

    /**
     * Closes the temporary file and attempts to promote it to the final file path. If the final
     * path already exists, the stream is canceled instead.
     *
     * @throws IOException if an error occurs during the under file system operation
     */
    private void complete() throws IOException {
      mStream.close();
      UnderFileSystem ufs = UnderFileSystem.get(mUri, mConfiguration);
      if (!ufs.rename(mTemporaryUri, mUri)) {
        // TODO(calvin): Log a warning if the delete fails
        ufs.delete(mTemporaryUri, false);
      }
    }

    /**
     * @return the wrapped output stream to the under file system file
     */
    private OutputStream getStream() {
      return mStream;
    }
  }

  // Input stream agent session index
  private final IndexedSet.FieldIndex<InputStreamAgent> mInputSessionIndex =
      new IndexedSet.FieldIndex<InputStreamAgent>() {
        @Override
        public Object getFieldValue(InputStreamAgent o) {
          return o.mSessionId;
        }
      };

  // Input stream agent id index
  private final IndexedSet.FieldIndex<InputStreamAgent> mInputIdIndex =
      new IndexedSet.FieldIndex<InputStreamAgent>() {
        @Override
        public Object getFieldValue(InputStreamAgent o) {
          return o.mId;
        }
      };

  // Output stream agent session index
  private final IndexedSet.FieldIndex<OutputStreamAgent> mOutputSessionIndex =
      new IndexedSet.FieldIndex<OutputStreamAgent>() {
        @Override
        public Object getFieldValue(OutputStreamAgent o) {
          return o.mSessionId;
        }
      };

  // Output stream agent id index
  private final IndexedSet.FieldIndex<OutputStreamAgent> mOutputIdIndex =
      new IndexedSet.FieldIndex<OutputStreamAgent>() {
        @Override
        public Object getFieldValue(OutputStreamAgent o) {
          return o.mId;
        }
      };

  /** A random id generator for worker file ids. */
  private final AtomicLong mIdGenerator;
  /** Map of worker file ids to open under file system input streams. */
  private final IndexedSet<InputStreamAgent> mInputAgents;
  /** Map of worker file ids to open under file system output streams. */
  private final IndexedSet<OutputStreamAgent> mOutputAgents;

  /**
   * Creates a new under file system manager. Stream ids are unique to each under file system
   * manager.
   */
  public UnderFileSystemManager() {
    mIdGenerator = new AtomicLong(IdUtils.getRandomNonNegativeLong());
    mInputAgents = new IndexedSet<>(mInputSessionIndex, mInputIdIndex);
    mOutputAgents = new IndexedSet<>(mOutputSessionIndex, mOutputIdIndex);
  }

  /**
   * Creates a {@link OutputStreamAgent} for the given file path and adds it to the open streams map
   * keyed with the returned worker file id.
   *
   * @param sessionId the session id of the request
   * @param ufsUri the path to create in the under file system
   * @return the worker file id which should be used to reference the open stream
   * @throws FileAlreadyExistsException if the under file system path already exists
   * @throws IOException if an error occurs when operating on the under file system
   */
  public long createFile(long sessionId, AlluxioURI ufsUri) throws FileAlreadyExistsException,
      IOException {
    long id = mIdGenerator.getAndIncrement();
    OutputStreamAgent stream =
        new OutputStreamAgent(sessionId, id, ufsUri, WorkerContext.getConf());
    mOutputAgents.add(stream);
    return id;
  }

  /**
   * Cancels the open stream associated with the given worker file id. This will close the open
   * stream.
   *
   * @param sessionId the session id of the request
   * @param tempUfsFileId the worker id referencing an open file in the under file system
   * @throws FileDoesNotExistException if the worker file id is not valid
   * @throws IOException if an error occurs when operating on the under file system
   */
  public void cancelFile(long sessionId, long tempUfsFileId)
      throws FileDoesNotExistException, IOException {
    OutputStreamAgent stream = mOutputAgents.remove(tempUfsFileId);
    if (stream != null) {
      stream.cancel();
    } else {
      throw new FileDoesNotExistException(
          ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(tempUfsFileId));
    }
  }

  /**
   * Closes an open input stream associated with the given temporary ufs file id. The temporary
   * ufs file id will be invalid after this is called.
   *
   * @param sessionId the session id of the request
   * @param tempUfsFileId the temporary ufs file id
   * @throws FileDoesNotExistException if the worker is not reading the specified file
   * @throws IOException if an error occurs when operating on the under file system
   */
  public void closeFile(long sessionId, long tempUfsFileId)
      throws FileDoesNotExistException, IOException {
    InputStreamAgent removed = mInputAgents.remove(tempUfsFileId);
    if (removed == null) {
      throw new FileDoesNotExistException(
          ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(tempUfsFileId));
    }
  }

  /**
   * Completes an open stream associated with the given worker file id. This will close the open
   * stream.
   *
   * @param sessionId the session id of the request
   * @param tempUfsFileId the worker id referencing an open file in the under file system
   * @throws FileDoesNotExistException if the worker file id is not valid
   * @throws IOException if an error occurs when operating on the under file system
   */
  public void completeFile(long sessionId, long tempUfsFileId)
      throws FileDoesNotExistException, IOException {
    OutputStreamAgent stream = mOutputAgents.remove(tempUfsFileId);
    if (stream == null) {
      throw new FileDoesNotExistException(
          ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(tempUfsFileId));
    }
    stream.complete();
  }

  /**
   * @param tempUfsFileId the temporary ufs file id
   * @param position the absolute position in the file to start the stream at
   * @return the input stream to read from this file
   * @throws FileDoesNotExistException if the worker file id not valid
   * @throws IOException if an error occurs when operating on the under file system
   */
  public InputStream getInputStreamAtPosition(long tempUfsFileId, long position)
      throws FileDoesNotExistException, IOException {
    InputStreamAgent stream = mInputAgents.get(tempUfsFileId);
    if (stream == null) {
      throw new FileDoesNotExistException(
          ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(tempUfsFileId));
    }
    return stream.openAtPosition(position);
  }

  /**
   * @param tempUfsFileId the temporary ufs file id
   * @return the output stream to write to this file
   * @throws FileDoesNotExistException if the worker file id not valid
   */
  public OutputStream getOutputStream(long tempUfsFileId) throws FileDoesNotExistException {
    OutputStreamAgent stream = mOutputAgents.get(tempUfsFileId);
    if (stream == null) {
      throw new FileDoesNotExistException(
          ExceptionMessage.BAD_WORKER_FILE_ID.getMessage(tempUfsFileId));
    }
    return stream.getStream();
  }

  /**
   * Opens a file from the under file system and associates it with a worker file id.
   *
   * @param sessionId the session id of the request
   * @param ufsUri the path to create in the under file system
   * @return the worker file id which should be used to reference the open stream
   * @throws FileDoesNotExistException if the file does not exist in the under file system
   * @throws IOException if an error occurs when operating on the under file system
   */
  public long openFile(long sessionId, AlluxioURI ufsUri)
      throws FileDoesNotExistException, IOException {
    InputStreamAgent stream = new InputStreamAgent(ufsUri, WorkerContext.getConf());
    long workerFileId = mIdGenerator.getAndIncrement();
    mInputAgents.put(workerFileId, stream);
    return workerFileId;
  }
}
