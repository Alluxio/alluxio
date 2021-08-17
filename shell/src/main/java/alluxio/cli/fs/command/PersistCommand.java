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

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.ThreadFactoryUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Persists files or directories currently stored only in Alluxio to the UnderFileSystem.
 */
@ThreadSafe
@PublicApi
public final class PersistCommand extends AbstractFileSystemCommand {
  private static final Logger LOG = LoggerFactory.getLogger(PersistCommand.class);
  private static final int DEFAULT_PARALLELISM = 4;
  private static final Option PARALLELISM_OPTION =
      Option.builder("p")
          .longOpt("parallelism")
          .argName("# concurrent operations")
          .numberOfArgs(1)
          .desc("Number of concurrent persist operations, default: " + DEFAULT_PARALLELISM)
          .required(false)
          .build();
  private static final int DEFAULT_TIMEOUT = 20 * Constants.MINUTE_MS;
  private static final Option TIMEOUT_OPTION =
      Option.builder("t")
          .longOpt("timeout")
          .argName("timeout in milliseconds")
          .numberOfArgs(1)
          .desc("Time in milliseconds for a single file persist to time out; default:"
              + DEFAULT_TIMEOUT)
          .required(false)
          .build();
  private static final int DEFAULT_WAIT_TIME = 0;
  private static final Option WAIT_OPTION =
      Option.builder("w")
          .longOpt("wait")
          .argName("the initial persistence wait time")
          .numberOfArgs(1)
          .desc("The time to wait before persisting. default: " + DEFAULT_WAIT_TIME)
          .required(false)
          .build();

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public PersistCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "persist";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(PARALLELISM_OPTION).addOption(TIMEOUT_OPTION)
        .addOption(WAIT_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }

  @Override
  public String getUsage() {
    return "persist [-p|--parallelism <#>] [-t|--timeout <milliseconds>] "
        + "[-w|--wait <milliseconds>] <path> [<path> ...]";
  }

  @Override
  public String getDescription() {
    return "Persists files or directories currently stored only in Alluxio to the UnderFileSystem.";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    // Parse arguments
    int parallelism = FileSystemShellUtils.getIntArg(cl, PARALLELISM_OPTION, DEFAULT_PARALLELISM);
    int timeoutMs = (int) FileSystemShellUtils.getMsArg(cl, TIMEOUT_OPTION, DEFAULT_TIMEOUT);
    long persistenceWaitTimeMs = FileSystemShellUtils.getMsArg(cl, WAIT_OPTION, DEFAULT_WAIT_TIME);

    // Validate arguments
    if ((persistenceWaitTimeMs > timeoutMs && timeoutMs != -1) || persistenceWaitTimeMs < 0) {
      System.out.println("Persistence initial wait time should be smaller than persist timeout "
          + "and bigger than zero");
      return -1;
    }

    String[] args = cl.getArgs();

    // Gather files to persist and enqueue them.
    List<AlluxioURI> candidateUris = new ArrayList<>();
    for (String path : args) {
      candidateUris.addAll(FileSystemShellUtils.getAlluxioURIs(mFileSystem, new AlluxioURI(path)));
    }
    final Queue<AlluxioURI> toPersist = new ConcurrentLinkedQueue<>();
    for (AlluxioURI uri : candidateUris) {
      queueNonPersistedRecursive(mFileSystem.getStatus(uri), toPersist);
    }
    int totalFiles = toPersist.size();
    System.out.println("Found " + totalFiles + " files to persist.");
    if (totalFiles == 0) {
      return 0;
    }

    // Launch persist tasks in parallel.
    parallelism = Math.min(totalFiles, parallelism);
    ExecutorService service =
        Executors.newFixedThreadPool(parallelism, ThreadFactoryUtils.build("persist-cli-%d", true));
    final Object progressLock = new Object();
    AtomicInteger completedFiles = new AtomicInteger(0);
    List<Future<Void>> futures = new ArrayList<>(parallelism);
    for (int i = 0; i < parallelism; i++) {
      futures.add(service.submit(new PersistCallable(toPersist, totalFiles, completedFiles,
          progressLock, persistenceWaitTimeMs, timeoutMs)));
    }

    // Await result.
    try {
      for (Future<Void> future : futures) {
        future.get();
      }
    } catch (ExecutionException e) {
      System.out.println("Fatal error: " + e);
      return -1;
    } catch (InterruptedException e) {
      System.out.println("Persist interrupted, exiting.");
      return -1;
    } finally {
      service.shutdownNow();
    }
    return 0;
  }

  private void queueNonPersistedRecursive(URIStatus status, Queue<AlluxioURI> toPersist)
      throws AlluxioException, IOException {
    AlluxioURI uri = new AlluxioURI(status.getPath());
    if (status.isFolder()) {
      List<URIStatus> statuses = mFileSystem.listStatus(uri);
      for (URIStatus s : statuses) {
        queueNonPersistedRecursive(s, toPersist);
      }
    } else if (!status.isPersisted()) {
      toPersist.add(uri);
    }
  }

  /**
   * Thread that polls a persist queue and persists files.
   */
  private class PersistCallable implements Callable<Void> {
    private final Queue<AlluxioURI> mFilesToPersist;
    private final int mTotalFiles;
    private final Object mProgressLock;
    private final AtomicInteger mCompletedFiles;
    private final long mPersistenceWaitTime;
    private final int mTimeoutMs;

    PersistCallable(Queue<AlluxioURI> toPersist, int totalFiles, AtomicInteger completedFiles,
        Object progressLock, long persistenceWaitTime, int timeoutMs) {
      mFilesToPersist = toPersist;
      mTotalFiles = totalFiles;
      mProgressLock = progressLock;
      mCompletedFiles = completedFiles;
      mPersistenceWaitTime = persistenceWaitTime;
      mTimeoutMs = timeoutMs;
    }

    @Override
    public Void call() throws Exception {
      AlluxioURI toPersist = mFilesToPersist.poll();
      while (toPersist != null) {
        try {
          FileSystemUtils.persistAndWait(mFileSystem, toPersist, mPersistenceWaitTime, mTimeoutMs);
          synchronized (mProgressLock) { // Prevents out of order progress tracking.
            String progress = "(" + mCompletedFiles.incrementAndGet() + "/" + mTotalFiles + ")";
            System.out.println(progress + " Successfully persisted file: " + toPersist);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.warn("Interrupted while waiting for persistence ", e);
          throw e;
        } catch (TimeoutException e) {
          String timeoutMsg =
              String.format("Timed out waiting for file to be persisted: %s", toPersist);
          System.out.println(timeoutMsg);
          LOG.error(timeoutMsg, e);
        } catch (Exception e) {
          System.out.println("Failed to persist file " + toPersist);
          LOG.error("Failed to persist file {}", toPersist, e);
        }
        toPersist = mFilesToPersist.poll();
      }
      return null;
    }
  }
}
