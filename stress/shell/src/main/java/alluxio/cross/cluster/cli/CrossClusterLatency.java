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
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.WritePType;

import com.google.common.base.Stopwatch;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class CrossClusterLatency extends CrossClusterBenchBase {

  final int mMakeFileCount;
  final int mRandReader;
  List<List<CrossClusterResultsRunning>> mCheckResults;
  final List<RandResult> mRandResult = new ArrayList<>();
  long mDuration;
  final List<CrossClusterResultsRunning> mTimers;

  CrossClusterLatency(AlluxioURI rootPath, List<List<InetSocketAddress>> clusterAddresses,
      int makeFileCount, long syncLatency, int randReaderThreadCount, WritePType writeType) {
    super(rootPath, "latencyFiles", clusterAddresses, syncLatency, writeType);
    mMakeFileCount = makeFileCount;
    mRandReader = randReaderThreadCount;
    mTimers = new ArrayList<>(mClients.size());
    for (int i = 0; i < mClients.size(); i++) {
      mTimers.add(new CrossClusterResultsRunning());
    }
  }

  void run() {
    System.out.println("Running bench");
    List<Thread> writerThreads = new ArrayList<>();
    List<Thread> randReaderThreads = new ArrayList<>();
    List<ExecutorService> executors = new ArrayList<>();
    mCheckResults = new ArrayList<>();
    for (int i = 0; i < mClients.size(); i++) {
      FileSystemCrossCluster mainClient = mClients.get(i);
      List<FileSystemCrossCluster> otherClients = new ArrayList<>();
      for (FileSystemCrossCluster client : mClients) {
        if (client != mainClient) {
          otherClients.add(client);
        }
      }
      AlluxioURI path = createClusterPath(mRootPath, i);
      AtomicBoolean running = new AtomicBoolean(true);
      ExecutorService executor = Executors.newFixedThreadPool(mClients.size() - 1);
      executors.add(executor);
      CrossClusterResultsRunning results = mTimers.get(i);
      mCheckResults.add(new ArrayList<>());
      for (int j = 0; j < otherClients.size(); j++) {
        mCheckResults.get(i).add(new CrossClusterResultsRunning());
      }
      int finalI = i;
      writerThreads.add(new Thread(() -> {
        try {
          runWriter(path, results, mCheckResults.get(finalI), mainClient, otherClients, executor);
        } catch (IOException e) {
          e.printStackTrace();
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          running.set(false);
        }
      }, String.format("Writer-cluster%d", i)));
      if (mRandReader > 0) {
        for (int j = 0; j < mRandReader; j++) {
          RandResult randResult = new RandResult();
          mRandResult.add(randResult);
          randReaderThreads.add(new Thread(() -> {
            try {
              runRandAccess(path, mRandReadClients.get(finalI), randResult, running);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }, String.format("RandReader-cluster%d-%d", i, j)));
        }
      }
    }
    Stopwatch duration = Stopwatch.createStarted();
    for (Thread writer : writerThreads) {
      writer.start();
    }
    for (Thread randReader : randReaderThreads) {
      randReader.start();
    }
    try {
      for (Thread writer : writerThreads) {
        writer.join();
      }
      for (Thread randReader : randReaderThreads) {
        randReader.join();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      mDuration = duration.elapsed(TimeUnit.MILLISECONDS);
      for (ExecutorService executor : executors) {
        executor.shutdown();
      }
      afterBench();
    }
  }

  LatencyResultsStatistics computeAllReadResults() {
    LatencyResultsStatistics results = new LatencyResultsStatistics();
    mCheckResults.stream().flatMap(Collection::stream)
        .forEach(nxt -> {
          try {
            results.merge(nxt.toResult());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    results.recordDuration(mDuration);
    return results;
  }

  RandResult computeAllRandResults() {
    RandResult result = mRandResult.stream().reduce(new RandResult(), RandResult::merge);
    result.setDuration(mDuration);
    return result;
  }

  void runRandAccess(AlluxioURI rootPath, FileSystemCrossCluster client,
                     RandResult result, AtomicBoolean running)
      throws Exception {
    Random rand = new Random();
    while (running.get()) {
      try {
        client.getStatus(createFileName(rootPath, rand.nextInt(mMakeFileCount)),
            mGetStatusOptions);
      } catch (FileDoesNotExistException e) {
        result.mFailures++;
        continue;
      }
      result.mSuccess++;
    }
  }

  void runWriter(
      AlluxioURI rootPath, CrossClusterResultsRunning latencyResults,
      List<CrossClusterResultsRunning> checkResults,
      FileSystemCrossCluster writeClient, List<FileSystemCrossCluster> readClients,
      ExecutorService executor) throws IOException, AlluxioException,
      ExecutionException, InterruptedException {
    List<Future<Void>> readResults = new ArrayList(Arrays.asList(new Object[readClients.size()]));

    for (int i = 0; i < mMakeFileCount; i++) {
      int finalI = i;
      // read the file on each cluster to check that it does not exist
      for (int j = 0; j < readClients.size(); j++) {
        int finalJ = j;
        readResults.set(j, executor.submit(() -> {
          long startNs = System.nanoTime();
          try {
            readClients.get(finalJ).getStatus(createFileName(rootPath, finalI), mGetStatusOptions);
          } catch (FileDoesNotExistException e) {
            // OK
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          checkResults.get(finalJ).opCompleted(System.nanoTime() - startNs);
          return null;
        }));
      }
      // wait for the reads to complete
      for (Future<Void> readResult : readResults) {
        readResult.get();
      }
      // write the file on the home cluster
      writeClient.createFile(createFileName(rootPath, i), mCreateFileOptions).close();
      // repeatably read the file on each cluster until it exists
      long viewLatency = System.nanoTime();
      for (int j = 0; j < readClients.size(); j++) {
        int finalJ = j;
        readResults.set(j, executor.submit(() -> {
          while (true) {
            long startNs = System.nanoTime();
            try {
              readClients.get(finalJ).getStatus(createFileName(rootPath, finalI),
                  mGetStatusOptions);
            } catch (FileDoesNotExistException e) {
              checkResults.get(finalJ).opCompleted(System.nanoTime() - startNs);
              continue;
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            checkResults.get(finalJ).opCompleted(System.nanoTime() - startNs);
            break;
          }
          return null;
        }));
      }
      // wait until all reads complete
      for (Future<Void> readResult : readResults) {
        readResult.get();
      }
      long durationNs = System.nanoTime() - viewLatency;
      latencyResults.opCompleted(durationNs);
      System.out.printf("Latency until visible on all clusters (ms): %f%n",
          durationNs / (float) MS_NANO);
    }
  }

  @Override
  CrossClusterLatencyStatistics getClientResults(int clientID) {
    return mTimers.get(clientID).toResult();
  }

  @Override
  Long getDurationMs() {
    return mDuration;
  }

  CrossClusterLatencyStatistics[] computeResults() throws Exception {
    CrossClusterLatencyStatistics[] results = super.computeResults();
    for (int k = 0; k < mClients.size(); k++) {
      if (mRandReader > 0) {
        RandResult randResult = new RandResult();
        for (int j = k * mRandReader; j < k * mRandReader + mRandReader; j++) {
          randResult = randResult.merge(mRandResult.get(j));
        }
        randResult.setDuration(mDuration);
        results[k].setRandResult(randResult);
      }
    }
    return results;
  }
}
