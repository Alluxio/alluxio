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
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.grpc.SetAttributePOptions;
import alluxio.proto.journal.File;

import com.google.protobuf.ByteString;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Sets the extends attributes of a file specified by args.
 */
@ThreadSafe
@PublicApi
public final class SetXAttrCommand extends AbstractFileSystemCommand {
  private static final String SET_FATTR = "setfattr";
  public static final String NAME = SET_FATTR;
  public static final String USAGE = "{-n name [-v value] | -x name} <path>";
  public static final String DESCRIPTION =
      "Sets an extended attribute name and value for a file or directory.\n"
          + "-n name: The extended attribute name.\n"
          + "-v value: The extended attribute value. There are three different "
          + "encoding methods for the value. If the argument is enclosed in double "
          + "quotes, then the value is the string inside the quotes. If the "
          + "argument is prefixed with 0x or 0X, then it is taken as a hexadecimal "
          + "number. If the argument begins with 0s or 0S, then it is taken as a "
          + "base64 encoding.\n"
          + "-x name: Remove the extended attribute.\n"
          + "<path>: The file or directory.\n";

  private static final Option NAME_OPTION =
      Option.builder("n")
          .required(false)
          .desc("extended attribute name")
          .build();
  private static final Option VALUE_OPTION =
      Option.builder("v")
          .required(false)
          .desc("extended attribute value")
          .build();
  private static final Option DELETE_NAME_OPTION =
      Option.builder("x")
          .required(false)
          .desc("delete extended attribute name")
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
    return NAME;
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(NAME_OPTION)
        .addOption(VALUE_OPTION)
        .addOption(DELETE_NAME_OPTION)
        .addOption(UPDATE_STRATEGY_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    SetAttributePOptions.Builder options = SetAttributePOptions.newBuilder();
    if (cl.hasOption(UPDATE_STRATEGY_OPTION.getOpt())) {
      options.setXattrUpdateStrategy(
          File.XAttrUpdateStrategy.valueOf(UPDATE_STRATEGY_OPTION.getValue()));
    }
    if (cl.hasOption(NAME_OPTION.getOpt())) {
      String name = cl.getOptionValue(NAME_OPTION.getOpt());
      String value;
      if (cl.hasOption(VALUE_OPTION.getOpt())) {
        value = cl.getOptionValue(VALUE_OPTION.getOpt());
        options.putXattr(name,
            ByteString.copyFrom(value.toString(), StandardCharsets.UTF_8));
      } else {
        options.setXattrUpdateStrategy(
            File.XAttrUpdateStrategy.DELETE_KEYS);
        options.putXattr(name, ByteString.EMPTY);
      }
    } else if (cl.hasOption(DELETE_NAME_OPTION.getOpt())) {
      String xname = cl.getOptionValue(DELETE_NAME_OPTION.getOpt());
      options.setXattrUpdateStrategy(
          File.XAttrUpdateStrategy.DELETE_KEYS);
      options.putXattr(xname, ByteString.EMPTY);
    }

    mSetAttributePOptions = options.build();
    AlluxioURI inputPath = new AlluxioURI(args[0]);
    runWildCardCmd(inputPath, cl);
    return 0;
  }

  @Override
  public String getUsage() {
    return USAGE;
  }

  @Override
  public String getDescription() {
    return DESCRIPTION;
  }
}
