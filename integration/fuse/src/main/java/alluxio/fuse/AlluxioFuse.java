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

import alluxio.ProjectConstants;
import alluxio.RuntimeConstants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.jnifuse.FuseException;
import alluxio.retry.RetryUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Main entry point to Alluxio-FUSE.
 */
@ThreadSafe
public final class AlluxioFuse {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFuse.class);

  private static final Option MOUNT_POINT_OPTION = Option.builder("m")
      .hasArg()
      .required(true)
      .longOpt("mount-point")
      .desc("Desired local mount point for alluxio-fuse.")
      .build();

  private static final Option ALLUXIO_ROOT_OPTION = Option.builder("r")
      .hasArg()
      .required(true)
      .longOpt("alluxio-root")
      .desc("Path within alluxio that will be used as the root of the FUSE mount "
          + "(e.g., /users/foo; defaults to /)")
      .build();

  private static final Option HELP_OPTION = Option.builder("h")
      .required(false)
      .desc("Print this help message")
      .build();

  private static final Option FUSE_MOUNT_OPTION = Option.builder("o")
      .valueSeparator(',')
      .required(false)
      .hasArgs()
      .desc("FUSE mount options")
      .build();

  private static final Options OPTIONS = new Options()
      .addOption(MOUNT_POINT_OPTION)
      .addOption(ALLUXIO_ROOT_OPTION)
      .addOption(HELP_OPTION)
      .addOption(FUSE_MOUNT_OPTION);

  // prevent instantiation
  private AlluxioFuse() {}

  /**
   * Running this class will mount the file system according to the options passed to this function
   * {@link #parseOptions(String[], AlluxioConfiguration)}. The user-space fuse application will
   * stay on the foreground and keep the file system mounted. The user can unmount the file system
   * by gracefully killing (SIGINT) the process.
   *
   * @param args arguments to run the command line
   */
  public static void main(String[] args) {
    LOG.info("Alluxio version: {}-{}", RuntimeConstants.VERSION, ProjectConstants.REVISION);
    AlluxioConfiguration conf = InstancedConfiguration.defaults();
    FileSystemContext fsContext = FileSystemContext.create(conf);
    try {
      InetSocketAddress confMasterAddress =
          fsContext.getMasterClientContext().getConfMasterInquireClient().getPrimaryRpcAddress();
      RetryUtils.retry("load cluster default configuration with master " + confMasterAddress,
          () -> fsContext.getClientContext().loadConfIfNotLoaded(confMasterAddress),
          RetryUtils.defaultClientRetry(
              conf.getDuration(PropertyKey.USER_RPC_RETRY_MAX_DURATION),
              conf.getDuration(PropertyKey.USER_RPC_RETRY_BASE_SLEEP_MS),
              conf.getDuration(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS)));
    } catch (IOException e) {
      LOG.warn("Failed to load cluster default configuration for Fuse process. "
          + "Proceed with local configuration for FUSE: {}", e.toString());
    }
    conf = fsContext.getClusterConf();
    final FuseMountInfo opts = parseOptions(args, conf);
    if (opts == null) {
      System.exit(1);
    }
    try {
      launchFuse(fsContext, opts, true);
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
  }

  /**
   * Launches Fuse application.
   *
   * @param fsContext file system context for Fuse client to communicate to servers
   * @param opts the fuse mount options
   * @param blocking whether the Fuse application is blocking or not
   * @return the Fuse application handler for future unmount operation
   */
  public static FuseUnmountable launchFuse(FileSystemContext fsContext,
      FuseMountInfo opts, boolean blocking) throws IOException {
    if (opts == null) {
      throw new IOException("The given fuse options cannot be null");
    }
    AlluxioConfiguration conf = fsContext.getClusterConf();
    try (final FileSystem fs = FileSystem.Factory.create(fsContext)) {
      final List<String> fuseOpts = opts.getFuseOpts();
      if (conf.getBoolean(PropertyKey.FUSE_JNIFUSE_ENABLED)) {
        final AlluxioJniFuseFileSystem fuseFs = new AlluxioJniFuseFileSystem(fs, opts, conf);
        try {
          LOG.info("Mounting AlluxioJniFuseFileSystem: mount point=\"{}\", OPTIONS=\"{}\"",
              opts.getMountPoint(), fuseOpts.toArray(new String[0]));
          fuseFs.mount(blocking, opts.isDebug(), fuseOpts.toArray(new String[0]));
        } catch (FuseException e) {
          // only try to umount file system when exception occurred.
          // jni-fuse registers JVM shutdown hook to ensure fs.umount()
          // will be executed when this process is exiting.
          fuseFs.umount();
          throw new IOException(String.format("Failed to mount %s", opts.getMountPoint()), e);
        }
        return fuseFs;
      } else {
        // Force direct_io in JNR-FUSE: writes and reads bypass the kernel page
        // cache and go directly to alluxio. This avoids extra memory copies
        // in the write path.
        // TODO(binfan): support kernel_cache (issues#10840)
        fuseOpts.add("-odirect_io");
        final AlluxioFuseFileSystem fuseFs = new AlluxioFuseFileSystem(fs, opts, conf);
        try {
          fuseFs.mount(Paths.get(opts.getMountPoint()), blocking, opts.isDebug(),
              fuseOpts.toArray(new String[0]));
        } catch (ru.serce.jnrfuse.FuseException e) {
          // only try to umount file system when exception occurred.
          // jnr-fuse registers JVM shutdown hook to ensure fs.umount()
          // will be executed when this process is exiting.
          fuseFs.umount();
          throw new IOException(String.format("Failed to mount %s", opts.getMountPoint()), e);
        }
        return fuseFs;
      }
    } catch (Throwable e) {
      throw new IOException("Failed to mount Alluxio file system", e);
    }
  }

  /**
   * Parses CLI options and gets fuse mount information.
   *
   * @param args CLI args
   * @return Alluxio-FUSE mount configuration information
   */
  @Nullable
  private static FuseMountInfo parseOptions(String[] args, AlluxioConfiguration alluxioConf) {
    final CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cli = parser.parse(OPTIONS, args);

      if (cli.hasOption("h")) {
        final HelpFormatter fmt = new HelpFormatter();
        fmt.printHelp(AlluxioFuse.class.getName(), OPTIONS);
        return null;
      }

      String mntPointValue = cli.getOptionValue("m");
      String alluxioRootValue = cli.getOptionValue("r");

      List<String> fuseOpts = new ArrayList<>();
      boolean noUserMaxWrite = true;
      if (cli.hasOption("o")) {
        String[] fopts = cli.getOptionValues("o");
        // keep the -o
        for (final String fopt : fopts) {
          fuseOpts.add("-o" + fopt);
          if (noUserMaxWrite && fopt.startsWith("max_write")) {
            noUserMaxWrite = false;
          }
        }
      }
      // check if the user has specified his own max_write, otherwise get it
      // from conf
      if (noUserMaxWrite) {
        final long maxWrite = alluxioConf.getBytes(PropertyKey.FUSE_MAXWRITE_BYTES);
        fuseOpts.add(String.format("-omax_write=%d", maxWrite));
      }

      final boolean fuseDebug = alluxioConf.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED);

      return new FuseMountInfo(mntPointValue, alluxioRootValue, fuseDebug, fuseOpts);
    } catch (ParseException e) {
      System.err.println("Error while parsing CLI: " + e.getMessage());
      final HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp(AlluxioFuse.class.getName(), OPTIONS);
      return null;
    }
  }
}
