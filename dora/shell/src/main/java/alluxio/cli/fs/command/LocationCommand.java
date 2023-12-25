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
import alluxio.client.file.DoraCacheFileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.wire.WorkerNetAddress;

import com.google.gson.Gson;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays a list of hosts that have the file specified in args stored.
 */
@ThreadSafe
@PublicApi
public final class LocationCommand extends AbstractFileSystemCommand {
  /**
   * Constructs a new instance to display a list of hosts that have the file specified in args
   * stored.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public LocationCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "location";
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    if (mFileSystem.getDoraCacheFileSystem() != null) {
      DoraCacheFileSystem doraCacheFileSystem = mFileSystem.getDoraCacheFileSystem();
      Map<String, List<WorkerNetAddress>> pathLocations =
          doraCacheFileSystem.checkFileLocation(plainPath);
      WorkerNetAddress preferredWorker = doraCacheFileSystem.getWorkerNetAddress(plainPath);

      AlluxioURI ufsFullPath = doraCacheFileSystem.convertToUfsPath(plainPath);
      String fileUfsFullName = ufsFullPath.toString();
      boolean dataOnPreferredWorker = false;
      Set<String> workersThatHaveDataSet = new HashSet<>();

      if (pathLocations != null && pathLocations.size() > 0) {
        Optional<String> fileUfsFullNameOpt = pathLocations.keySet().stream().findFirst();
        if (fileUfsFullNameOpt.isPresent()) {
          List<WorkerNetAddress> workersThatHaveDataList = pathLocations.get(fileUfsFullName);
          if (workersThatHaveDataList != null && !workersThatHaveDataList.isEmpty()) {
            dataOnPreferredWorker = workersThatHaveDataList.contains(preferredWorker);
            workersThatHaveDataSet = workersThatHaveDataList.stream()
                .map(WorkerNetAddress::getHost).collect(Collectors.toSet());
          }
        }
      }

      FileLocation fileLocation = new FileLocation(
          fileUfsFullName,
          preferredWorker.getHost(),
          dataOnPreferredWorker,
          workersThatHaveDataSet);

      Gson gson = new Gson();
      String pathLocationsJson = gson.toJson(fileLocation);
      System.out.println(pathLocationsJson);
    }
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);
    return 0;
  }

  @Override
  public String getUsage() {
    return "location <path>";
  }

  @Override
  public String getDescription() {
    return "Displays the list of hosts storing the specified file.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }
}
