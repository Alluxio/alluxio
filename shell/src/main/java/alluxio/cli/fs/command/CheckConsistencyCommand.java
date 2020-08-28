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
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.URIStatus;
import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.AggregateException;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.resource.CloseableResource;
import alluxio.util.FileSystemOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Command for checking the consistency of a file or folder between Alluxio and the under storage.
 */
@PublicApi
public class CheckConsistencyCommand extends AbstractFileSystemCommand {

  private static final Option REPAIR_OPTION =
      Option.builder("r")
          .required(false)
          .hasArg(false)
          .desc("repair inconsistent files")
          .build();

  private static final Option THREADS_OPTION =
      Option.builder("t")
          .longOpt("threads")
          .required(false)
          .hasArg(true)
          .desc("Number of threads used when repairing consistency. Defaults to <number of cores>"
              + " * 2. This option has no effect if -r is not specified")
          .build();

  private static final String PARSE_THREADS_FAILURE_FMT = "The threads option must be a positive "
      + "integer but was \"%s\"";

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public CheckConsistencyCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    int threads;
    try {
      threads = cl.hasOption(THREADS_OPTION.getOpt())
          ? Integer.parseInt(cl.getOptionValue(THREADS_OPTION.getOpt())) :
          Runtime.getRuntime().availableProcessors() * 2;
      if (threads < 1) {
        throw new IOException(String.format(PARSE_THREADS_FAILURE_FMT, THREADS_OPTION.getOpt()));
      }
    } catch (NumberFormatException e) {
      throw new IOException(String.format(PARSE_THREADS_FAILURE_FMT, THREADS_OPTION.getOpt()));
    }
    runConsistencyCheck(plainPath, cl.hasOption("r"), threads);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(REPAIR_OPTION).addOption(THREADS_OPTION);
  }

  @Override
  public String getCommandName() {
    return "checkConsistency";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI root = new AlluxioURI(args[0]);

    runWildCardCmd(root, cl);
    return 0;
  }

  /**
   * Checks the consistency of Alluxio metadata against the under storage for all files and
   * directories in a given subtree.
   *
   * @param path the root of the subtree to check
   * @return a list of inconsistent files and directories
   */
  List<AlluxioURI> checkConsistency(AlluxioURI path, CheckConsistencyPOptions options)
      throws IOException {
    try (CloseableResource<FileSystemMasterClient> client =
        mFsContext.acquireMasterClientResource()) {
      return client.get().checkConsistency(path, options);
    }
  }

  /**
   * Checks the inconsistent files and directories which exist in Alluxio but don't exist in the
   * under storage, repairs the inconsistent paths by deleting them if repairConsistency is true.
   *
   * @param path the specified path to be checked
   * @param repairConsistency whether to repair the consistency or not
   * @throws AlluxioException
   * @throws IOException
   */
  private void runConsistencyCheck(AlluxioURI path, boolean repairConsistency, int repairThreads)
      throws AlluxioException, IOException {
    List<AlluxioURI> inconsistentUris =
        checkConsistency(path, FileSystemOptions.checkConsistencyDefaults(
            mFsContext.getPathConf(path)));
    if (inconsistentUris.isEmpty()) {
      System.out.println(path + " is consistent with the under storage system.");
      return;
    }
    if (!repairConsistency) {
      Collections.sort(inconsistentUris);
      System.out.println("The following files are inconsistent:");
      for (AlluxioURI uri : inconsistentUris) {
        System.out.println(uri);
      }
    } else {
      Collections.sort(inconsistentUris);
      System.out.println(String.format("%s has: %d inconsistent files. Repairing with %d threads.",
          path, inconsistentUris.size(), repairThreads));
      ConcurrentHashSet<AlluxioURI> inconsistentDirs = new ConcurrentHashSet<>();

      ExecutorService svc = Executors.newFixedThreadPool(repairThreads);
      CompletionService<Boolean> completionService = new ExecutorCompletionService<>(svc);
      ConcurrentHashSet<Exception> exceptions = new ConcurrentHashSet<>();

      int totalUris = inconsistentUris.size();
      for (AlluxioURI inconsistentUri : inconsistentUris) {
        completionService.submit(() -> {
          try {
            URIStatus status = mFileSystem.getStatus(inconsistentUri);
            if (status.isFolder()) {
              inconsistentDirs.add(inconsistentUri);
              return;
            }
            System.out.println("repairing path: " + inconsistentUri);
            DeletePOptions deleteOptions = DeletePOptions.newBuilder().setAlluxioOnly(true).build();
            mFileSystem.delete(inconsistentUri, deleteOptions);
            mFileSystem.exists(inconsistentUri);
            System.out.println(inconsistentUri + " repaired");
            System.out.println();
          } catch (AlluxioException | IOException e) {
            exceptions.add(e);
          }
        }, true);
      }

      waitForTasks(completionService, totalUris, exceptions);

      int totalDirs = inconsistentDirs.size();
      for (AlluxioURI uri : inconsistentDirs) {
        completionService.submit(() -> {
          try {
            DeletePOptions deleteOptions =
                DeletePOptions.newBuilder().setAlluxioOnly(true).setRecursive(true).build();
            System.out.println("repairing path: " + uri);
            mFileSystem.delete(uri, deleteOptions);
            mFileSystem.exists(uri);
            System.out.println(uri + "repaired");
            System.out.println();
          } catch (AlluxioException | IOException e) {
            exceptions.add(e);
          }
        }, true);
      }
      waitForTasks(completionService, totalDirs, exceptions);
      svc.shutdown();
    }
  }

  private void waitForTasks(CompletionService svc, int nTasks, Collection<Exception> exceptions)
      throws IOException {
    for (int i = 0; i < nTasks; i++) {
      try {
        svc.take();
      } catch (InterruptedException e) {
        throw new IOException("Failed to wait for all URIs to complete");
      }
    }

    if (exceptions.size() > 0) {
      AggregateException e = new AggregateException(exceptions);
      throw new IOException("Failed to successfully repair all paths", e);
    }
  }

  @Override
  public String getUsage() {
    return "checkConsistency [-r] [-t|--threads <threads>] <Alluxio path>";
  }

  @Override
  public String getDescription() {
    return "Checks the consistency of a persisted file or directory in Alluxio. Any files or "
        + "directories which only exist in Alluxio or do not match the metadata of files in the "
        + "under storage will be returned. An administrator should then reconcile the  "
        + "differences. Specify -r to repair the inconsistent files. Use -t or --threads to "
        + "specify the number of threads that should be used when repairing. Defaults to "
        + "2*<number of CPU cores>";
  }
}
