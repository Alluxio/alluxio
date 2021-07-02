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
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays ACL info of a path.
 */
@ThreadSafe
@PublicApi
public final class GetFaclCommand extends AbstractFileSystemCommand {
  /**
   * @param fsContext the filesystem of Alluxio
   */
  public GetFaclCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "getfacl";
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(path);
    System.out.println("# file: " + status.getPath());
    System.out.println("# owner: " + status.getOwner());
    System.out.println("# group: " + status.getGroup());
    for (String entry : status.getAcl().toStringEntries()) {
      System.out.println(entry);
    }
    List<String> defaultAclEntries = status.getDefaultAcl().toStringEntries();
    for (String entry: defaultAclEntries) {
      System.out.println(entry);
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
    return "getfacl <path>";
  }

  @Override
  public String getDescription() {
    return "Displays the access control lists (ACLs) for a path.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }
}
