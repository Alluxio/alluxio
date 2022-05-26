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

package alluxio.fuse;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * An object that holds all the options from the command line
 * when Alluxio fuse is being launched through CLI.
 */
public final class AlluxioFuseCliOpts {
  private final String mMountPoint;
  private final String mAlluxioPath;
  private final List<String> mLibfuseOptions;

  private AlluxioFuseCliOpts(@Nullable String mountPoint, @Nullable String alluxioPath,
      @Nullable List<String> libfuseOptions) {
    mMountPoint = mountPoint;
    mAlluxioPath = alluxioPath;
    mLibfuseOptions = libfuseOptions;
  }

  /**
   * @return The path to where the FS should be mounted
   */
  public Optional<String> getMountPoint() {
    return mMountPoint == null ? Optional.empty() : Optional.of(mMountPoint);
  }

  /**
   * @return The path within alluxio that will be mounted to the local mount point
   */
  public Optional<String> getMountAlluxioPath() {
    return mAlluxioPath == null ? Optional.empty() : Optional.of(mAlluxioPath);
  }

  /**
   * @return extra options to pass to the FUSE mount command
   */
  public Optional<List<String>> getFuseOptions() {
    return mLibfuseOptions == null ? Optional.empty() : Optional.of(mLibfuseOptions);
  }

  /**
   * A parser that parses the CLI when Fuse is launched through command line.
   */
  public static class AlluxioFuseCliParser {

    private static final CommandLineParser PARSER = new DefaultParser();

    private static final String MOUNT_POINT_OPTION_NAME = "m";
    private static final String MOUNT_ALLUXIO_PATH_OPTION_NAME = "a";
    private static final String MOUNT_OPTIONS_OPTION_NAME = "o";
    private static final String HELP_OPTION_NAME = "h";

    private static final Option MOUNT_POINT_OPTION = Option.builder(MOUNT_POINT_OPTION_NAME)
        .hasArg()
        .required(false)
        .longOpt("mount-point")
        .desc("The absolute local filesystem path that standalone Fuse will mount Alluxio path to.")
        .build();
    private static final Option MOUNT_ALLUXIO_PATH_OPTION
        = Option.builder(MOUNT_ALLUXIO_PATH_OPTION_NAME)
        .hasArg()
        .required(false)
        .longOpt("alluxio-path")
        .desc("The Alluxio path to mount to the given Fuse mount point "
              + "(e.g., /users/foo; defaults to /)")
        .build();
    private static final Option MOUNT_OPTIONS = Option.builder(MOUNT_OPTIONS_OPTION_NAME)
        .valueSeparator(',')
        .required(false)
        .hasArgs()
        .desc("FUSE mount options")
        .build();
    private static final Option HELP_OPTION = Option.builder(HELP_OPTION_NAME)
        .required(false)
        .desc("Print this help message")
        .build();
    private static final Options OPTIONS = new Options()
        .addOption(MOUNT_POINT_OPTION)
        .addOption(MOUNT_ALLUXIO_PATH_OPTION)
        .addOption(MOUNT_OPTIONS)
        .addOption(HELP_OPTION);

    /**
     * Constructs a {@link AlluxioFuseCliOpts} based on user command line input.
     *
     * @param args     the fuse command line arguments
     * @return an AlluxioFuseCliOpts object holding all command line arguments
     */
    public static AlluxioFuseCliOpts parseAndCreateAlluxioFuseCliOpts(String[] args) {
      try {
        CommandLine cli = PARSER.parse(OPTIONS, args);

        if (cli.hasOption("h")) {
          final HelpFormatter fmt = new HelpFormatter();
          fmt.printHelp(AlluxioFuseCliParser.class.getName(), OPTIONS);
          return null;
        }

        String mountPoint = cli.getOptionValue(MOUNT_POINT_OPTION_NAME);
        String mountAlluxioPath = cli.getOptionValue(MOUNT_ALLUXIO_PATH_OPTION_NAME);
        List<String> libfuseOpts = cli.hasOption(MOUNT_OPTIONS_OPTION_NAME)
            ? Arrays.asList(cli.getOptionValues(MOUNT_OPTIONS_OPTION_NAME)) : null;

        return new AlluxioFuseCliOpts(mountPoint, mountAlluxioPath, libfuseOpts);
      } catch (ParseException e) {
        System.err.println("Error while parsing CLI: " + e.getMessage());
        final HelpFormatter fmt = new HelpFormatter();
        fmt.printHelp(AlluxioFuseCliParser.class.getName(), OPTIONS);
        return null;
      }
    }
  }
}
