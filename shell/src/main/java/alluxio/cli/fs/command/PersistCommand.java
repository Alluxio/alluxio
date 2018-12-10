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
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Persists files or directories currently stored only in Alluxio to the UnderFileSystem.
 */
@ThreadSafe
public final class PersistCommand extends AbstractFileSystemCommand {
  private static final Option PARALLELISM_OPTION =
      Option.builder()
          .longOpt("parallelism")
          .argName("# threads")
          .numberOfArgs(1)
          .desc("Number of concurrent persist operations.")
          .required(false)
          .build();

  private final BlockingQueue<AlluxioURI> mToPersist;
  private final AtomicInteger mFilesPersisted;
  /** Total number of files to persist in a run. Should only be set by main thread. */
  private int mTotalFilesToPersist;

  /**
   * @param fs the filesystem of Alluxio
   */
  public PersistCommand(FileSystem fs) {
    super(fs);
    mToPersist = new LinkedBlockingQueue<>();
    mFilesPersisted = new AtomicInteger(0);
    mTotalFilesToPersist = 0;
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
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(plainPath);
    queuePersist(status);
  }

  private void queuePersist(URIStatus status) throws AlluxioException, IOException {
    AlluxioURI uri = new AlluxioURI(status.getPath());
    if (status.isFolder()) {
      List<URIStatus> statuses = mFileSystem.listStatus(uri);
      for (URIStatus s : statuses) {
        queuePersist(s);
      }
    } else if (status.isPersisted()) {
      System.out.println(status.getPath() + " is already persisted, skipping.");
    } else {
      mToPersist.add(uri);
    }
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    resetState();
    int parallelism = 1;
    if (cl.hasOption(PARALLELISM_OPTION.getLongOpt())) {
      String parellismOption = cl.getOptionValue(PARALLELISM_OPTION.getLongOpt());
      parallelism = Integer.parseInt(parellismOption);
    }
    String[] args = cl.getArgs();
    for (String path : args) {
      AlluxioURI inputPath = new AlluxioURI(path);
      runWildCardCmd(inputPath, cl);
    }
    mTotalFilesToPersist = mToPersist.size();
    System.out.println("Found " + mTotalFilesToPersist + " files to persist.");
    ExecutorService service = Executors.newFixedThreadPool(parallelism);
    List<Future<Void>> persistCalls = new ArrayList<>(parallelism);
    for (int i = 0; i < parallelism; i++) {
      persistCalls.add(service.submit(new PersistCallable()));
    }
    try {
      for (Future<Void> call : persistCalls) {
        call.get();
      }
    } catch (ExecutionException e) {
      System.out.println("Fatal error: " + e);
    } catch (InterruptedException e) {
      System.out.println("Persist interrupted, exiting.");
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "persist [--parallelism <#>] <path> [<path> ...]";
  }

  @Override
  public String getDescription() {
    return "Persists files or directories currently stored only in Alluxio to the "
        + "UnderFileSystem.";
  }

  /**
   * Clears the queue to persist and internal variables. This class is not designed to be used
   * concurrently or from an existing incomplete state.
   */
  private void resetState() {
    mToPersist.clear();
    mFilesPersisted.set(0);
    mTotalFilesToPersist = 0;
  }

  /**
   * Thread that polls the persist queue and persists files.
   */
  private class PersistCallable implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      AlluxioURI toPersist = mToPersist.poll();
      while (toPersist != null) {
        try {
          FileSystemUtils.persistFile(mFileSystem, toPersist);
          // There is a slight chance of out of order output to the client.
          // If this becomes a problem we can consider locking instead of an atomic int.
          String progress =
              "(" + mFilesPersisted.incrementAndGet() + "/" + mTotalFilesToPersist + ")";
          System.out.println(progress + " Persisted file " + toPersist);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        } catch (TimeoutException e) {
          throw new RuntimeException(e);
        } catch (Exception e) {
          System.out.println("Failed to persist file " + toPersist);
        }
        toPersist = mToPersist.poll();
      }
      return null;
    }
  }
}
