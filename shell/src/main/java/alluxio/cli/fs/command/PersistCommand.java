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
import alluxio.cli.CommandUtils;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.client.file.FileSystem;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Persists files or directories currently stored only in Alluxio to the UnderFileSystem.
 */
@ThreadSafe
public final class PersistCommand extends AbstractFileSystemCommand {
  private static final Logger LOG = LoggerFactory.getLogger(PersistCommand.class);
  private static final Option PARALLELISM_OPTION =
      Option.builder("p")
          .longOpt("parallelism")
          .argName("# threads")
          .numberOfArgs(1)
          .desc("Number of concurrent persist operations.")
          .required(false)
          .build();

  /**
   * @param fs the filesystem of Alluxio
   */
  public PersistCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "persist";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(PARALLELISM_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }

  @Override
  public String getUsage() {
    return "persist [-p|--parallelism <#>] <path> [<path> ...]";
  }

  @Override
  public String getDescription() {
    return "Persists files or directories currently stored only in Alluxio to the UnderFileSystem.";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    int parallelism = 4;
    if (cl.hasOption(PARALLELISM_OPTION.getLongOpt())) {
      String parellismOption = cl.getOptionValue(PARALLELISM_OPTION.getLongOpt());
      parallelism = Integer.parseInt(parellismOption);
    }
    String[] args = cl.getArgs();
    List<AlluxioURI> candidateUris = new ArrayList<>();
    for (String path : args) {
      candidateUris.addAll(FileSystemShellUtils.getAlluxioURIs(mFileSystem, new AlluxioURI(path)));
    }
    final BlockingQueue<AlluxioURI> toPersist = new LinkedBlockingQueue<>();
    for (AlluxioURI uri : candidateUris) {
      queuePersist(mFileSystem.getStatus(uri), toPersist);
    }
    System.out.println("Found " + toPersist.size() + " files to persist.");
    ExecutorService service =
        Executors.newFixedThreadPool(parallelism, ThreadFactoryUtils.build("persist-cli-%d", true));
    final Object progressLock = new Object();
    AtomicInteger completedFiles = new AtomicInteger(0);
    List<Future<Void>> futures = new ArrayList<>(parallelism);
    for (int i = 0; i < parallelism; i++) {
      futures.add(service.submit(new PersistCallable(toPersist, completedFiles, progressLock)));
    }
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

  private void queuePersist(URIStatus status, BlockingQueue<AlluxioURI> toPersist)
      throws AlluxioException, IOException {
    AlluxioURI uri = new AlluxioURI(status.getPath());
    if (status.isFolder()) {
      List<URIStatus> statuses = mFileSystem.listStatus(uri);
      for (URIStatus s : statuses) {
        queuePersist(s, toPersist);
      }
    } else if (!status.isPersisted()) {
      toPersist.add(uri);
    }
  }

  /**
   * Thread that polls a persist queue and persists files.
   */
  private class PersistCallable implements Callable<Void> {
    private final BlockingQueue<AlluxioURI> mToPersist;
    private final int mTotalFiles;
    private final Object mProgressLock;
    private final AtomicInteger mCompletedFiles;

    PersistCallable(BlockingQueue<AlluxioURI> toPersist, AtomicInteger count, Object progressLock) {
      mToPersist = toPersist;
      mTotalFiles = toPersist.size();
      mProgressLock = progressLock;
      mCompletedFiles = count;
    }

    @Override
    public Void call() throws Exception {
      AlluxioURI toPersist = mToPersist.poll();
      while (toPersist != null) {
        try {
          FileSystemUtils.persistFile(mFileSystem, toPersist);
          synchronized (mProgressLock) { // Prevents out of order progress tracking.
            String progress = "(" + mCompletedFiles.incrementAndGet() + "/" + mTotalFiles + ")";
            System.out.println(progress + " Successfully persisted file: " + toPersist);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw e;
        } catch (Exception e) {
          System.out.println("Failed to persist file " + toPersist);
          LOG.error("Failed to persist file {}", toPersist, e);
        }
        toPersist = mToPersist.poll();
      }
      return null;
    }
  }
}
