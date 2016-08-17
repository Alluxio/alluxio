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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Main entry point to Alluxio-FUSE.
 */
@ThreadSafe
public final class AlluxioFuse {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  // prevent instantiation
  private AlluxioFuse() {
  }

  /**
   * Running this class will mount the file system according to
   * the options passed to this function {@link #parseOptions(String[])}.
   * The user-space fuse application will stay on the foreground and keep
   * the file system mounted. The user can unmount the file system by
   * gracefully killing (SIGINT) the process.
   *
   * @param args arguments to run the command line
   */
  public static void main(String[] args) {
    final AlluxioFuseOptions opts = parseOptions(args);
    if (opts == null) {
      System.exit(1);
    }

    final FileSystem tfs = FileSystem.Factory.get();
    final AlluxioFuseFileSystem fs = new AlluxioFuseFileSystem(tfs, opts);
    final List<String> fuseOpts = opts.getFuseOpts();
    // Force direct_io in FUSE: writes and reads bypass the kernel page
    // cache and go directly to alluxio. This avoids extra memory copies
    // in the write path.
    fuseOpts.add("-odirect_io");

    try {
      fs.mount(Paths.get(opts.getMountPoint()), true, opts.isDebug(),
          fuseOpts.toArray(new String[0]));
    } finally {
      fs.umount();
    }
  }

  /**
   * Parses CLI options.
   *
   * @param args CLI args
   * @return Alluxio-FUSE configuration options
   */
  private static AlluxioFuseOptions parseOptions(String[] args) {
    final Options opts = new Options();
    final Option mntPoint = Option.builder("m")
        .hasArg()
        .required(false)
        .longOpt("mount-point")
        .desc("Desired local mount point for alluxio-fuse.")
        .build();

    final Option alluxioRoot = Option.builder("r")
        .hasArg()
        .required(false)
        .longOpt("alluxio-root")
        .desc("Path within alluxio that will be used as the root of the FUSE mount "
            + "(e.g., /users/foo; defaults to /)")
        .build();

    final Option help = Option.builder("h")
        .required(false)
        .desc("Print this help")
        .build();

    final Option fuseOption = Option.builder("o")
        .valueSeparator(',')
        .required(false)
        .hasArgs()
        .desc("FUSE mount options")
        .build();

    opts.addOption(mntPoint);
    opts.addOption(alluxioRoot);
    opts.addOption(help);
    opts.addOption(fuseOption);

    final CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cli = parser.parse(opts, args);

      if (cli.hasOption("h")) {
        final HelpFormatter fmt = new HelpFormatter();
        fmt.printHelp(AlluxioFuse.class.getName(), opts);
        return null;
      }

      String mntPointValue = cli.getOptionValue("m");
      String alluxioRootValue = cli.getOptionValue("r");

      List<String> fuseOpts = new ArrayList<>();
      boolean noUserMaxWrite = true;
      if (cli.hasOption("o")) {
        String[] fopts = cli.getOptionValues("o");
        // keep the -o
        for (final String fopt: fopts) {
          fuseOpts.add("-o" + fopt);
          if (noUserMaxWrite && fopt.startsWith("max_write")) {
            noUserMaxWrite = false;
          }
        }
      }
      // check if the user has specified his own max_write, otherwise get it
      // from conf
      if (noUserMaxWrite) {
        final long maxWrite = Configuration.getLong(PropertyKey.FUSE_MAXWRITE_BYTES);
        fuseOpts.add(String.format("-omax_write=%d", maxWrite));
      }

      if (mntPointValue == null) {
        mntPointValue = Configuration.get(PropertyKey.FUSE_MOUNT_DEFAULT);
        LOG.info("Mounting on default {}", mntPointValue);
      }

      if (alluxioRootValue == null) {
        alluxioRootValue = Configuration.get(PropertyKey.FUSE_FS_ROOT);
        LOG.info("Using default alluxio root {}", alluxioRootValue);
      }

      final boolean fuseDebug = Configuration.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED);

      return new AlluxioFuseOptions(mntPointValue, alluxioRootValue, fuseDebug, fuseOpts);
    } catch (ParseException e) {
      System.err.println("Error while parsing CLI: " + e.getMessage());
      final HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp(AlluxioFuse.class.getName(), opts);
      return null;
    }
  }
}
