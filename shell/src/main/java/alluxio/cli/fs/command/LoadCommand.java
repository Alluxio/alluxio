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
import alluxio.cli.CommandUtils;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads a file or directory in Alluxio space, making it resident in Alluxio.
 */
@ThreadSafe
public final class LoadCommand extends AbstractFileSystemCommand {
  private static final Option LOCAL_OPTION =
      Option.builder()
          .longOpt("local")
          .required(false)
          .hasArg(false)
          .desc("load the file to local worker.")
          .build();

  /**
   * Constructs a new instance to load a file or directory in Alluxio space.
   *
   * @param fs the filesystem of Alluxio
   */
  public LoadCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "load";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(LOCAL_OPTION);
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    load(plainPath, cl.hasOption(LOCAL_OPTION.getLongOpt()));
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);

    return 0;
  }

  /**
   * Loads a file or directory in Alluxio space, makes it resident in Alluxio.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio
   * @param local whether to load data to local worker even when the data is already loaded remotely
   */
  private void load(AlluxioURI filePath, boolean local) throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(filePath);
    if (status.isFolder()) {
      List<URIStatus> statuses = mFileSystem.listStatus(filePath);
      for (URIStatus uriStatus : statuses) {
        AlluxioURI newPath = new AlluxioURI(uriStatus.getPath());
        load(newPath, local);
      }
    } else {
      OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.CACHE_PROMOTE);
      if (local) {
        if (!FileSystemContext.INSTANCE.hasLocalWorker()) {
          System.out.println("When local option is specified,"
              + " there must be a local worker available");
          return;
        }
        options.setCacheLocationPolicy(new LocalFirstPolicy());
      } else if (status.getInAlluxioPercentage() == 100) {
        // The file has already been fully loaded into Alluxio.
        System.out.println(filePath + " already in Alluxio fully");
        return;
      }
      Closer closer = Closer.create();
      try {
        FileInStream in = closer.register(mFileSystem.openFile(filePath, options));
        byte[] buf = new byte[8 * Constants.MB];
        while (in.read(buf) != -1) {
        }
      } catch (Exception e) {
        throw closer.rethrow(e);
      } finally {
        closer.close();
      }
    }
    System.out.println(filePath + " loaded");
  }

  @Override
  public String getUsage() {
    return "load [--local] <path>";
  }

  @Override
  public String getDescription() {
    return "Loads a file or directory in Alluxio space, makes it resident in Alluxio.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }
}
