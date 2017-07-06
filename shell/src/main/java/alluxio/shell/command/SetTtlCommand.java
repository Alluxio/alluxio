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
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import alluxio.wire.TtlAction;

import com.google.common.base.Preconditions;
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
  public Options getOptions() {
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
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    long ttlMs = Long.parseLong(CommonUtils.stripLeadingAndTrailingQuotes(args[1]));
    Preconditions.checkArgument(ttlMs >= 0, "TTL value must be >= 0");
    AlluxioURI path = new AlluxioURI(args[0]);
    CommandUtils.setTtl(mFileSystem, path, ttlMs, mAction);
    System.out.println("TTL of path '" + path + "' was successfully set to " + ttlMs
        + " milliseconds, with expiry action set to " + mAction);
    return 0;
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
