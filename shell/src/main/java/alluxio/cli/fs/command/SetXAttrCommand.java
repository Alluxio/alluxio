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
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.SetAttributePOptions;
import alluxio.proto.journal.File;

import com.google.protobuf.ByteString;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Sets the extends attributes of a file specified by args.
 */
@ThreadSafe
@PublicApi
public final class SetXAttrCommand extends AbstractFileSystemCommand {

  private static final Option XATTR_OPTION =
      Option.builder()
          .longOpt("xattr")
          .required(false)
          .hasArg(true)
          .numberOfArgs(2)
          .argName("key=value")
          .valueSeparator('=')
          .desc("extended attribute")
          .build();
  private static final Option UPDATE_STRATEGY_OPTION =
      Option.builder("s").longOpt("strategy")
          .required(false)
          .numberOfArgs(1)
          .desc("update xattr strategy")
          .build();

  private SetAttributePOptions mSetAttributePOptions;

  /**
   * Creates a new instance of {@link SetXAttrCommand}.
   *
   * @param fsContext an Alluxio file system handle
   */
  public SetXAttrCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    mFileSystem.setAttribute(path, mSetAttributePOptions);
  }

  @Override
  public String getCommandName() {
    return "setxattr";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(XATTR_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    SetAttributePOptions.Builder options = SetAttributePOptions.newBuilder();
    if (cl.hasOption(UPDATE_STRATEGY_OPTION.getOpt())) {
      options.setXattrUpdateStrategy(
          File.XAttrUpdateStrategy.valueOf(UPDATE_STRATEGY_OPTION.getValue()));
    }
    if (cl.hasOption(XATTR_OPTION.getLongOpt())) {
      Properties properties = cl.getOptionProperties(XATTR_OPTION.getLongOpt());
      properties.forEach((k, v) -> {
        options.putXattr(k.toString(),
            ByteString.copyFrom(v.toString(), StandardCharsets.UTF_8));
      });
    }

    mSetAttributePOptions = options.build();
    AlluxioURI inputPath = new AlluxioURI(args[0]);
    runWildCardCmd(inputPath, cl);
    return 0;
  }

  @Override
  public String getUsage() {
    return "setxattr [-xattr KEY=VALUE] <path>";
  }

  @Override
  public String getDescription() {
    return "Set xattr for specific path.";
  }
}
