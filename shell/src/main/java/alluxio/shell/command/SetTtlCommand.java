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
import alluxio.Configuration;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Sets a new TTL value for the file at path both of the TTL value and the path are specified by
 * args.
 */
@ThreadSafe
public final class SetTtlCommand extends AbstractShellCommand {

  /**
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public SetTtlCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
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
  public void run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    long ttlMs = Long.parseLong(args[1]);
    Preconditions.checkArgument(ttlMs >= 0, "TTL value must be >= 0");
    AlluxioURI path = new AlluxioURI(args[0]);
    CommandUtils.setTtl(mFileSystem, path, ttlMs);
    System.out.println("TTL of file '" + path + "' was successfully set to " + ttlMs
        + " milliseconds.");
  }

  @Override
  public String getUsage() {
    return "setTtl <path> <time to live(in milliseconds)>";
  }

  @Override
  public String getDescription() {
    return "Sets a new TTL value for the file at path.";
  }
}
