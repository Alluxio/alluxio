/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
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
import alluxio.PropertyKey;
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
import alluxio.worker.block.BlockWorker;
import alluxio.worker.file.options.CompleteUfsFileOptions;
import alluxio.worker.file.options.CreateUfsFileOptions;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.thrift.TProcessor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is responsible for managing all top level components of the file system worker.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class DefaultFileSystemWorker extends AbstractWorker implements FileSystemWorker {
  /** Logic for managing file persistence. */
  private final FileDataManager mFileDataManager;
  /** Client for file system master communication. */
  private final FileSystemMasterClient mFileSystemMasterWorkerClient;
  /** Logic for handling RPC requests. */
  private final FileSystemWorkerClientServiceHandler mServiceHandler;
  /** Object for managing this worker's sessions. */
  private final Sessions mSessions;
  /** Runnable responsible for clean up potential zombie sessions. */
  private final SessionCleaner mSessionCleaner;
  /** Manager for under file system operations. */
  private final UnderFileSystemManager mUnderFileSystemManager;
  /** This worker's worker ID. May be updated by another thread if worker re-registration occurs. */
  private final AtomicReference<Long> mWorkerId;

  /** The service that persists files. */
  private Future<?> mFilePersistenceService;

  /**
   * Creates a new instance of {@link FileSystemWorker}.
   *
   * @param blockWorker the block worker handle
   * @param workerId a reference to the id of this worker
   * @throws IOException if an I/O error occurs
   */
  public DefaultFileSystemWorker(BlockWorker blockWorker, AtomicReference<Long> workerId)
      throws IOException {
    super(Executors.newFixedThreadPool(3,
        ThreadFactoryUtils.build("file-system-worker-heartbeat-%d", true)));
    mWorkerId = workerId;
    mSessions = new Sessions();
    mUnderFileSystemManager = new UnderFileSystemManager();
    mFileDataManager = new FileDataManager(Preconditions.checkNotNull(blockWorker),
        RateLimiter.create(
            Configuration.getBytes(PropertyKey.WORKER_FILE_PERSIST_RATE_LIMIT)));

    // Setup AbstractMasterClient
    mFileSystemMasterWorkerClient = new FileSystemMasterClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC));

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

  @Override
  public void cancelUfsFile(long sessionId, long tempUfsFileId)
      throws FileDoesNotExistException, IOException {
    mUnderFileSystemManager.cancelFile(sessionId, tempUfsFileId);
  }

  @Override
  public void closeUfsFile(long sessionId, long tempUfsFileId)
      throws FileDoesNotExistException, IOException {
    mUnderFileSystemManager.closeFile(sessionId, tempUfsFileId);
  }

  @Override
  public long completeUfsFile(long sessionId, long tempUfsFileId, CompleteUfsFileOptions options)
      throws FileDoesNotExistException, IOException {
    return mUnderFileSystemManager.completeFile(sessionId, tempUfsFileId, options);
  }

  @Override
  public long createUfsFile(long sessionId, AlluxioURI ufsUri, CreateUfsFileOptions options)
      throws FileAlreadyExistsException, IOException {
    return mUnderFileSystemManager.createFile(sessionId, ufsUri, options);
  }

  @Override
  public InputStream getUfsInputStream(long tempUfsFileId, long position)
      throws FileDoesNotExistException, IOException {
    return mUnderFileSystemManager.getInputStreamAtPosition(tempUfsFileId, position);
  }

  @Override
  public OutputStream getUfsOutputStream(long tempUfsFileId) throws FileDoesNotExistException {
    return mUnderFileSystemManager.getOutputStream(tempUfsFileId);
  }

  @Override
  public FileSystemWorkerClientServiceHandler getWorkerServiceHandler() {
    return mServiceHandler;
  }

  @Override
  public long openUfsFile(long sessionId, AlluxioURI ufsUri)
      throws FileDoesNotExistException, IOException {
    return mUnderFileSystemManager.openFile(sessionId, ufsUri);
  }

  @Override
  public void sessionHeartbeat(long sessionId) {
    mSessions.sessionHeartbeat(sessionId);
  }

  @Override
  public void start() {
    mFilePersistenceService = getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_FILESYSTEM_MASTER_SYNC,
            new FileWorkerMasterSyncExecutor(mFileDataManager, mFileSystemMasterWorkerClient,
                mWorkerId),
            Configuration.getInt(PropertyKey.WORKER_FILESYSTEM_HEARTBEAT_INTERVAL_MS)));

    // Start the session cleanup checker to perform the periodical checking
    getExecutorService().submit(mSessionCleaner);
  }

  @Override
  public void stop() {
    mSessionCleaner.stop();
    if (mFilePersistenceService != null) {
      mFilePersistenceService.cancel(true);
    }
    mFileSystemMasterWorkerClient.close();
    // This needs to be shutdownNow because heartbeat threads will only stop when interrupted.
    getExecutorService().shutdownNow();
  }
}
