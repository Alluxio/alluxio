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

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.wire.TtlAction;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Sets a new TTL value for the file at path both of the TTL value and the path are specified by
 * args.
 */
@ThreadSafe
public final class SetTtlCommand extends AbstractShellCommand {

  private static final String TTL_ACTION = "action";

  private static final Option TTL_ACTION_OPTION = Option.builder(TTL_ACTION).required(false)
      .numberOfArgs(1).desc("Action to take after Ttl expiry").build();

  private TtlAction mAction = TtlAction.DELETE;

  /**
   * @param fs the filesystem of Alluxio
   */
  public SetTtlCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "setTtl";
  }

  @Override
  protected int getNumOfArgs() {
    return 2;
  }

  @Override
  protected Options getOptions() {
    return new Options().addOption(TTL_ACTION_OPTION);
  }

  @Override
  public CommandLine parseAndValidateArgs(String... args) {

    CommandLine cmd = super.parseAndValidateArgs(args);
    try {
      String operation = cmd.getOptionValue(TTL_ACTION);
      if (operation != null) {
        mAction = TtlAction.valueOf(operation.toUpperCase());
      }
    } catch (Exception e) {
      System.err.println("action should be delete OR free");
      cmd = null;
    }
    return cmd;
  }

  @Override
  public void run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    long ttlMs = Long.parseLong(args[1]);
    Preconditions.checkArgument(ttlMs >= 0, "TTL value must be >= 0");
    AlluxioURI path = new AlluxioURI(args[0]);
    setTtl(path, ttlMs);
  }

  /**
   * setTtl to a file or directory.
   * @param filePath the {@link AlluxioURI} path to setTtl
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void setTtl(AlluxioURI filePath, long ttlMs) throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(filePath);
    if (status.isFolder()) {
      List<URIStatus> statuses = mFileSystem.listStatus(filePath);
      List<String> errorMessages = new ArrayList<>();
      for (URIStatus uriStatus : statuses) {
        AlluxioURI newPath = new AlluxioURI(uriStatus.getPath());
        try {
          setTtl(newPath, ttlMs);
        } catch (IOException e) {
          errorMessages.add(e.getMessage());
        }
      }
      if (errorMessages.size() != 0) {
        throw new IOException(Joiner.on('\n').join(errorMessages));
      }
    } else {
      CommandUtils.setTtl(mFileSystem, filePath, ttlMs, mAction);
      System.out.println("TTL of file '" + filePath + "' was successfully set to " + ttlMs
          + " milliseconds, with expiry action set to " + mAction);
    }
  }

  @Override
  public String getUsage() {
    return "setTtl [-action delete|free] <path> <time to live(in milliseconds)>";
  }

  @Override
  public String getDescription() {
    return "Sets a new TTL value for the file at path, "
        + "performing an action, delete(Default)/free after Ttl expiry.";
  }
}
