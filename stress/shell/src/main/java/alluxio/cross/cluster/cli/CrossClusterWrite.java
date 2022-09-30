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

package alluxio.cross.cluster.cli;

import static alluxio.Constants.MS_NANO;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.WritePType;

import com.google.common.util.concurrent.RateLimiter;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

class CrossClusterWrite extends CrossClusterBenchBase {

  final RateLimiter[] mRateLimiter;
  final long mFolderSize;
  private final long mDuration;
  private final CountDownLatch mStarted = new CountDownLatch(1);
  private final CountDownLatch mReady;
  private final AtomicBoolean mRunning = new AtomicBoolean(true);
  private final List<List<WriteThread>> mWriterThreads;
  private long mStartTimeNs;
  private long mEndTimeNs;

  class WriteThread implements Runnable {
    final int mClusterId;
    final int mThreadId;
    final FileSystemCrossCluster mClient;
    final AlluxioURI mThreadPath;
    final CrossClusterResultsRunning mResult = new CrossClusterResultsRunning();

    WriteThread(int clusterId, int threadId, FileSystemCrossCluster client) {
      mClusterId = clusterId;
      mThreadId = threadId;
      mClient = client;
      mThreadPath = createThreadPath(mRootPath, mClusterId, mThreadId);
    }

    CrossClusterResultsRunning getResult() {
      return mResult;
    }

    void doSetup() {
      try {
        mClient.createDirectory(mThreadPath,
            CreateDirectoryPOptions.newBuilder().setRecursive(true)
                .setWriteType(WritePType.CACHE_THROUGH).build());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void run() {
      try {
        long nxtFolderId = 0;
        long nxtFileId = 0;
        AlluxioURI nxtPath = mThreadPath.join(Long.toString(nxtFolderId));
        mClient.createDirectory(nxtPath);
        mReady.countDown();
        mStarted.await();
        while (mRunning.get()) {
          if (mRateLimiter != null) {
            mRateLimiter[mClusterId].acquire();
          }
          long startNs = System.nanoTime();
          mClient.createFile(nxtPath.join(Long.toString(nxtFileId)), mCreateFileOptions).close();
          mResult.opCompleted(System.nanoTime() - startNs);
          nxtFileId++;
          if (nxtFileId % mFolderSize == 0) {
            nxtFileId = 0;
            nxtFolderId++;
            nxtPath = mThreadPath.join(Long.toString(nxtFolderId));
            mClient.createDirectory(nxtPath);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(String.format("Exception in thread %d for cluster %d",
            mThreadId, mClusterId), e);
      }
    }
  }

  static AlluxioURI createThreadPath(AlluxioURI rootPath, int clusterId, int threadId) {
    return createClusterPath(rootPath, clusterId).join(Integer.toString(threadId));
  }

  CrossClusterWrite(AlluxioURI rootPath, List<List<InetSocketAddress>> clusterAddresses,
                    int writeThreads, long duration, long syncLatency, @Nullable Long maxRate,
                    long folderSize, boolean singleWriter, WritePType writeType) {
    super(rootPath, "write", clusterAddresses, syncLatency, writeType);
    if (maxRate != null) {
      mRateLimiter = new RateLimiter[mClients.size()];
      for (int i = 0; i < mRateLimiter.length; i++) {
        mRateLimiter[i] = RateLimiter.create(maxRate);
      }
    } else {
      mRateLimiter = null;
    }
    mFolderSize = folderSize;
    mDuration = duration;
    int numWriterClusters;
    if (singleWriter) {
      numWriterClusters = 1;
    } else {
      numWriterClusters = clusterAddresses.size();
    }
    mWriterThreads = new ArrayList<>(numWriterClusters);
    for (int i = 0; i < numWriterClusters; i++) {
      mWriterThreads.add(new ArrayList<>(writeThreads));
      for (int j = 0; j < writeThreads; j++) {
        mWriterThreads.get(i).add(new WriteThread(i, j, mClients.get(i)));
      }
    }
    mReady = new CountDownLatch((int) mWriterThreads.stream().mapToLong(Collection::size).sum());
  }

  @Override
  CrossClusterLatencyStatistics getClientResults(int clientID) {
    if (clientID >= mWriterThreads.size()) {
      return new CrossClusterLatencyStatistics();
    }
    return mWriterThreads.get(clientID).stream().map(WriteThread::getResult)
        .map(CrossClusterResultsRunning::toResult).reduce(new CrossClusterLatencyStatistics(),
            (acc, nxt) -> {
              try {
                acc.merge(nxt);
                return acc;
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }

  @Override
  Long getDurationMs() {
    return (mEndTimeNs - mStartTimeNs) / MS_NANO;
  }

  void doSetup() throws Exception {
    super.doSetup();
    // first list the directory recursively, so we don't need a sync
    for (FileSystemCrossCluster client : mClients) {
      client.listStatus(mRootPath, ListStatusPOptions.newBuilder().setRecursive(true)
          .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
              .setSyncIntervalMs(0).buildPartial()).build());
    }
    for (List<WriteThread> writer : mWriterThreads) {
      writer.forEach(WriteThread::doSetup);
    }
  }

  void run() {
    System.out.println("Running bench");
    List<Thread> threads = new ArrayList<>(mWriterThreads.size());
    for (List<WriteThread> writeThread : mWriterThreads) {
      writeThread.forEach(nxt -> {
        Thread thread = new Thread(nxt);
        thread.start();
        threads.add(thread);
      });
    }
    try {
      mReady.await();
      mStartTimeNs = System.nanoTime();
      mStarted.countDown();
      Thread.sleep(mDuration);
      mRunning.set(false);
      for (Thread thread : threads) {
        thread.join();
      }
      mEndTimeNs = System.nanoTime();
      afterBench();
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for bench to finish", e);
    }
  }
}
