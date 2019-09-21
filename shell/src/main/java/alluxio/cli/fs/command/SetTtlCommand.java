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
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.TtlAction;
import alluxio.util.CommonUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Sets a new TTL value for the file at path both of the TTL value and the path are specified by
 * args.
 */
@ThreadSafe
@PublicApi
public final class SetTtlCommand extends AbstractFileSystemCommand {

  private static final String TTL_ACTION = "action";

  private static final Option TTL_ACTION_OPTION =
      Option.builder()
          .longOpt(TTL_ACTION)
          .required(false)
          .numberOfArgs(1)
          .desc("Action to take after TTL expiry. Delete (default) or free the target")
          .build();

  private TtlAction mAction = TtlAction.DELETE;
  private long mTtlMs;

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public SetTtlCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "setTtl";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 2);
    String operation = cl.getOptionValue(TTL_ACTION);
    if (operation != null) {
      try {
        mAction = TtlAction.valueOf(operation.toUpperCase());
      } catch (Exception e) {
        throw new InvalidArgumentException(String.format("TTL action should be %s OR %s, not %s",
            TtlAction.DELETE, TtlAction.FREE, operation));
      }
    }
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(TTL_ACTION_OPTION);
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    FileSystemCommandUtils.setTtl(mFileSystem, path, mTtlMs, mAction);
    System.out.println("TTL of path '" + path + "' was successfully set to " + mTtlMs
            + " milliseconds, with expiry action set to " + mAction);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    String ttl = CommonUtils.stripLeadingAndTrailingQuotes(args[1]);
    mTtlMs = FileSystemShellUtils.getMs(ttl);
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);

    return 0;
  }

  @Override
  public String getUsage() {
    return "setTtl [--action delete|free] <path> <time to live>";
  }

  @Override
  public String getDescription() {
    return "Sets a new TTL value for the file at path, "
        + "performing an action, delete(default)/free after TTL expiry. "
        + "The TTL to set can be in one of the unit: ms, millisecond, s, second, m, min, minute, "
        + "h, hour, d, day, default to ms";
  }
}
