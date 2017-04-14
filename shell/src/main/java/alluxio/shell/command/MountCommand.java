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
import alluxio.client.file.options.MountOptions;
import alluxio.exception.AlluxioException;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Mounts a UFS path onto an Alluxio path.
 */
@ThreadSafe
public final class MountCommand extends AbstractShellCommand {
  private static final Pattern OPTION_PATTERN = Pattern.compile("(.*)=(.*)");

  private static final Option READONLY_OPTION =
      Option.builder()
          .longOpt("readonly")
          .required(false)
          .hasArg(false)
          .desc("mount point is readonly in Alluxio")
          .build();
  private static final Option SHARED_OPTION =
      Option.builder()
          .longOpt("shared")
          .required(false)
          .hasArg(false)
          .desc("mount point is shared")
          .build();
  private static final Option OPTION_OPTION =
      Option.builder()
          .longOpt("option")
          .argName("key=value")
          .required(false)
          .hasArg(true)
          .desc("options associated with this mount point")
          .build();
  // TODO(gpang): Investigate property=value style of cmdline options. They didn't seem to
  // support spaces in values.
  private static final Option PROPERTY_FILE_OPTION =
      Option.builder("P")
          .required(false)
          .numberOfArgs(1)
          .desc("properties file name")
          .build();

  /**
   * @param fs the filesystem of Alluxio
   */
  public MountCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "mount";
  }

  @Override
  protected int getNumOfArgs() {
    return 2;
  }

  @Override
  protected Options getOptions() {
    return new Options().addOption(PROPERTY_FILE_OPTION).addOption(READONLY_OPTION)
        .addOption(SHARED_OPTION).addOption(OPTION_OPTION);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI alluxioPath = new AlluxioURI(args[0]);
    AlluxioURI ufsPath = new AlluxioURI(args[1]);
    MountOptions options = MountOptions.defaults();

    String propertyFile = cl.getOptionValue('P');
    if (propertyFile != null) {
      Properties cmdProps = new Properties();
      try (InputStream inStream = new FileInputStream(propertyFile)) {
        cmdProps.load(inStream);
      } catch (IOException e) {
        throw new IOException("Unable to load property file: " + propertyFile);
      }

      if (!cmdProps.isEmpty()) {
        // Use the properties from the properties file for the mount options.
        Map<String, String> properties = new HashMap<>();
        for (Map.Entry<Object, Object> entry : cmdProps.entrySet()) {
          properties.put(entry.getKey().toString(), entry.getValue().toString());
        }
        options.setProperties(properties);
        System.out.println("Using properties from file: " + propertyFile);
      }
    }

    if (cl.hasOption("readonly")) {
      options.setReadOnly(true);
    }

    if (cl.hasOption("shared")) {
      options.setShared(true);
    }

    if (cl.hasOption("option")) {
      Map<String, String> properties = new HashMap<>();
      String[] optionValues = cl.getOptionValues("option");
      for (String option : optionValues) {
        Matcher matcher = OPTION_PATTERN.matcher(option);
        Preconditions.checkArgument(matcher.matches(), "Unrecognized property %s", option);
        String key = matcher.group(1);
        String value = matcher.group(2);
        properties.put(key, value);
      }
      options.setProperties(properties);
    }

    mFileSystem.mount(alluxioPath, ufsPath, options);
    System.out.println("Mounted " + ufsPath + " at " + alluxioPath);
    return 0;
  }

  @Override
  public String getUsage() {
    return "mount [--readonly] [--shared] [--option <key=val>] [-P <path>] <alluxioPath> <ufsURI>";
  }

  @Override
  public String getDescription() {
    return "Mounts a UFS path onto an Alluxio path.";
  }
}
