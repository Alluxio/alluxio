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
import alluxio.Constants;
import alluxio.Sessions;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.thrift.FileSystemWorkerClientService;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.worker.AbstractWorker;
import alluxio.worker.SessionCleaner;
import alluxio.worker.SessionCleanupCallback;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;
import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is responsible for managing all top level components of the file system worker.
 */
// TODO(calvin): Add session concept
// TODO(calvin): Reconsider the naming of the ufs operations
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class FileSystemWorker extends AbstractWorker {
  /** Logic for managing file persistence. */
  private final FileDataManager mFileDataManager;
  /** Client for file system master communication. */
  private final FileSystemMasterClient mFileSystemMasterWorkerClient;
  /** Configuration object. */
  private final Configuration mConf;
  /** Logic for handling RPC requests. */
  private final FileSystemWorkerClientServiceHandler mServiceHandler;
  /** Object for managing this worker's sessions. */
  private final Sessions mSessions;
  /** Runnable responsible for clean up potential zombie sessions. */
  private final SessionCleaner mSessionCleaner;
  /** Manager for under file system operations. */
  private final UnderFileSystemManager mUnderFileSystemManager;

  /** The service that persists files. */
  private Future<?> mFilePersistenceService;

  /**
   * Creates a new instance of {@link FileSystemWorker}.
   *
   * @param blockWorker the block worker handle
   * @throws IOException if an I/O error occurs
   */
  public FileSystemWorker(BlockWorker blockWorker) throws IOException {
    super(Executors.newFixedThreadPool(3,
        ThreadFactoryUtils.build("file-system-worker-heartbeat-%d", true)));

    mConf = WorkerContext.getConf();
    mSessions = new Sessions();
    mFileDataManager = new FileDataManager(Preconditions.checkNotNull(blockWorker));
    mUnderFileSystemManager = new UnderFileSystemManager();

    // Setup AbstractMasterClient
    mFileSystemMasterWorkerClient = new FileSystemMasterClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, mConf), mConf);

    // Setup session cleaner
    mSessionCleaner = new SessionCleaner(new SessionCleanupCallback() {
      /**
       * Cleans up after sessions, to prevent zombie sessions holding ufs resources.
       */
      @Override
      public void cleanupSessions() {
        for (long session : mSessions.getTimedOutSessions()) {
          mSessions.removeSession(session);
          mUnderFileSystemManager.cleanupSession(session);
        }
      }
    });

    mServiceHandler = new FileSystemWorkerClientServiceHandler(this);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(
        Constants.FILE_SYSTEM_WORKER_CLIENT_SERVICE_NAME,
        new FileSystemWorkerClientService.Processor<>(getWorkerServiceHandler()));
    return services;
  }

  /**
   * Cancels a file currently being written to the under file system. The open stream will be
   * closed and the partial file will be cleaned up.
   *
   * @param sessionId the session id of the request
   * @param tempUfsFileId the id of the file to cancel, only understood by the worker that created
   *                      the file
   * @throws FileDoesNotExistException if this worker is not writing the specified file
   * @throws IOException if an error occurs interacting with the under file system
   */
  public void cancelUfsFile(long sessionId, long tempUfsFileId)
      throws FileDoesNotExistException, IOException {
    mUnderFileSystemManager.cancelFile(sessionId, tempUfsFileId);
  }

  /**
   * Closes a file currently being read from the under file system. The open stream will be
   * closed and the file id will no longer be valid.
   *
   * @param sessionId the session id of the request
   * @param tempUfsFileId the id of the file to close, only understood by the worker that opened
   *                      the file
   * @throws FileDoesNotExistException if the worker is not reading the specified file
   * @throws IOException if an error occurs interacting with the under file system
   */
  public void closeUfsFile(long sessionId, long tempUfsFileId)
      throws FileDoesNotExistException, IOException {
    mUnderFileSystemManager.closeFile(sessionId, tempUfsFileId);
  }

  /**
   * Completes a file currently being written to the under file system. The open stream will be
   * closed and the partial file will be promoted to the completed file in the under file system.
   *
   * @param sessionId the session id of the request
   * @param tempUfsFileId the id of the file to complete, only understood by the worker that created
   *                      the file
   * @return the length of the completed file
   * @throws FileDoesNotExistException if the worker is not writing the specified file
   * @throws IOException if an error occurs interacting with the under file system
   */
  public long completeUfsFile(long sessionId, long tempUfsFileId)
      throws FileDoesNotExistException, IOException {
    return mUnderFileSystemManager.completeFile(sessionId, tempUfsFileId);
  }

  /**
   * Creates a new file in the under file system. This will register a new stream in the under
   * file system manager. The stream can only be accessed with the returned id afterward.
   *
   * @param sessionId the session id of the request
   * @param ufsUri the under file system uri to create a file for
   * @throws FileAlreadyExistsException if a file already exists in the under file system with
   *                                    the same path
   * @throws IOException if an error occurs interacting with the under file system
   * @return the temporary worker specific file id which references the in-progress ufs file
   */
  public long createUfsFile(long sessionId, AlluxioURI ufsUri)
      throws FileAlreadyExistsException, IOException {
    return mUnderFileSystemManager.createFile(sessionId, ufsUri);
  }

  /**
   * Opens a stream to the under file system file denoted by the temporary file id. This call
   * will skip to the position specified in the file before returning the stream. The caller of
   * this method is required to close the stream after they have finished operations on it.
   *
   * @param tempUfsFileId the worker specific temporary file id for the file in the under storage
   * @param position the absolute position in the file to position the stream at before returning
   * @return an input stream to the ufs file positioned at the given position
   * @throws FileDoesNotExistException if the worker file id is invalid
   * @throws IOException if an error occurs interacting with the under file system
   */
  public InputStream getUfsInputStream(long tempUfsFileId, long position)
      throws FileDoesNotExistException, IOException {
    return mUnderFileSystemManager.getInputStreamAtPosition(tempUfsFileId, position);
  }

  /**
   * Returns the output stream to the under file system file denoted by the temporary file id.
   * The stream should not be closed by the caller but through the {@link #cancelUfsFile(long,long)}
   * or the {@link #completeUfsFile(long,long)} methods.
   *
   * @param tempUfsFileId the worker specific temporary file id for the file in the under storage
   * @return the output stream writing the contents of the file
   * @throws FileDoesNotExistException if the temporary file id is invalid
   */
  public OutputStream getUfsOutputStream(long tempUfsFileId) throws FileDoesNotExistException {
    return mUnderFileSystemManager.getOutputStream(tempUfsFileId);
  }

  /**
   * @return the worker service handler
   */
  public FileSystemWorkerClientServiceHandler getWorkerServiceHandler() {
    return mServiceHandler;
  }

  /**
   * Opens a file in the under file system and registers a temporary file id with the file. This
   * id is valid until the file is closed.
   *
   * @param sessionId the session id of the request
   * @param ufsUri the under file system path of the file to open
   * @return the temporary file id which references the file
   * @throws FileDoesNotExistException if the file does not exist in the under file system
   * @throws IOException if an error occurs interacting with the under file system
   */
  public long openUfsFile(long sessionId, AlluxioURI ufsUri)
      throws FileDoesNotExistException, IOException {
    return mUnderFileSystemManager.openFile(sessionId, ufsUri);
  }

  /**
   * Registers a client's heartbeat to keep its session alive. The client can also
   * piggyback metrics on this call. Currently there are no metrics collected from this call.
   *
   * @param sessionId the session id to renew
   * @param metrics a list of metrics to update from the client
   */
  public void sessionHeartbeat(long sessionId, List<Long> metrics) {
    // Metrics currently ignored
    mSessions.sessionHeartbeat(sessionId);
  }

  /**
   * Starts the filesystem worker service.
   */
  @Override
  public void start() {
    mFilePersistenceService = getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC,
            new FileWorkerMasterSyncExecutor(mFileDataManager, mFileSystemMasterWorkerClient),
            mConf.getInt(Constants.WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS)));

    // Start the session cleanup checker to perform the periodical checking
    getExecutorService().submit(mSessionCleaner);
  }

  /**
   * Stops the filesystem worker service.
   */
  @Override
  public void stop() {
    if (mFilePersistenceService != null) {
      mFilePersistenceService.cancel(true);
    }
    mFileSystemMasterWorkerClient.close();
    getExecutorService().shutdown();
  }
}
