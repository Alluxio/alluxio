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
import alluxio.conf.ServerConfiguration;
import alluxio.jnifuse.FuseException;
import alluxio.metrics.MetricsSystem;
import alluxio.retry.RetryUtils;
import alluxio.util.CommonUtils;
import alluxio.util.io.FileUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

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
    final FuseMountOptions opts = parseOptions(args, conf);
    if (opts == null) {
      System.exit(1);
    }
    CommonUtils.PROCESS_TYPE.set(CommonUtils.ProcessType.CLIENT);
    MetricsSystem.startSinks(conf.get(PropertyKey.METRICS_CONF_FILE));
    if (conf.getBoolean(PropertyKey.FUSE_WEB_ENABLED)) {
      FuseWebServer webServer = new FuseWebServer(
          NetworkAddressUtils.ServiceType.FUSE_WEB.getServiceName(),
          NetworkAddressUtils.getBindAddress(
              NetworkAddressUtils.ServiceType.FUSE_WEB,
              ServerConfiguration.global()));
      webServer.start();
    }
    try (FileSystem fs = FileSystem.Factory.create(fsContext)) {
      FuseUmountable fuseUmountable = launchFuse(fs, conf, opts, true);
    } catch (IOException e) {
      LOG.error("Failed to launch FUSE", e);
      System.exit(-1);
    }
  }

  /**
   * Launches Fuse application.
   *
   * @param fs file system for Fuse client to communicate to servers
   * @param conf the alluxio configuration to create Fuse file system
   * @param opts the fuse mount options
   * @param blocking whether the Fuse application is blocking or not
   * @return the Fuse application handler for future Fuse umount operation
   */
  public static FuseUmountable launchFuse(FileSystem fs, AlluxioConfiguration conf,
      FuseMountOptions opts, boolean blocking) throws IOException {
    Preconditions.checkNotNull(opts,
        "Fuse mount options should not be null to launch a Fuse application");
    try {
      String mountPoint = opts.getMountPoint();
      if (!FileUtils.exists(mountPoint)) {
        LOG.warn("Mount point on local fs does not exist, creating {}", mountPoint);
        FileUtils.createDir(mountPoint);
      }
      final List<String> fuseOpts = opts.getFuseOpts();
      if (conf.getBoolean(PropertyKey.FUSE_JNIFUSE_ENABLED)) {
        final AlluxioJniFuseFileSystem fuseFs = new AlluxioJniFuseFileSystem(fs, opts, conf);

        FuseSignalHandler fuseSignalHandler = new FuseSignalHandler(fuseFs);
        Signal.handle(new Signal("TERM"), fuseSignalHandler);

        try {
          LOG.info("Mounting AlluxioJniFuseFileSystem: mount point=\"{}\", OPTIONS=\"{}\"",
              opts.getMountPoint(), fuseOpts.toArray(new String[0]));
          fuseFs.mount(blocking, opts.isDebug(), fuseOpts.toArray(new String[0]));
          return fuseFs;
        } catch (FuseException e) {
          // only try to umount file system when exception occurred.
          // jni-fuse registers JVM shutdown hook to ensure fs.umount()
          // will be executed when this process is exiting.
          fuseFs.umount(true);
          throw new IOException(String.format("Failed to mount alluxio path %s to mount point %s",
              opts.getAlluxioRoot(), opts.getMountPoint()), e);
        }
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
          return fuseFs;
        } catch (ru.serce.jnrfuse.FuseException e) {
          // only try to umount file system when exception occurred.
          // jnr-fuse registers JVM shutdown hook to ensure fs.umount()
          // will be executed when this process is exiting.
          fuseFs.umount();
          throw new IOException(String.format("Failed to mount alluxio path %s to mount point %s",
              opts.getAlluxioRoot(), opts.getMountPoint()), e);
        }
      }
    } catch (Throwable e) {
      throw new IOException("Failed to mount Alluxio file system", e);
    }
  }

  /**
   * Parses CLI options.
   *
   * @param args CLI args
   * @return Alluxio-FUSE configuration options
   */
  @Nullable
  private static FuseMountOptions parseOptions(String[] args, AlluxioConfiguration alluxioConf) {
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

      List<String> fuseOpts = parseFuseOptions(
          cli.hasOption("o") ? cli.getOptionValues("o") : new String[0], alluxioConf);

      final boolean fuseDebug = alluxioConf.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED);

      return new FuseMountOptions(mntPointValue, alluxioRootValue, fuseDebug, fuseOpts);
    } catch (ParseException e) {
      System.err.println("Error while parsing CLI: " + e.getMessage());
      final HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp(AlluxioFuse.class.getName(), OPTIONS);
      return null;
    }
  }

  /**
   * Parses user given fuse options to the format that Fuse application needs.
   *
   * @param fuseOptions the fuse options to parse from
   * @param alluxioConf alluxio configuration
   * @return the parsed fuse options
   */
  public static List<String> parseFuseOptions(String[] fuseOptions,
      AlluxioConfiguration alluxioConf) {
    List<String> res = new ArrayList<>();
    boolean noUserMaxWrite = true;
    for (final String opt : fuseOptions) {
      if (opt.isEmpty()) {
        continue;
      }
      res.add("-o" + opt);
      if (noUserMaxWrite && opt.startsWith("max_write")) {
        noUserMaxWrite = false;
      }
    }
    // check if the user has specified his own max_write, otherwise get it
    // from conf
    if (noUserMaxWrite) {
      final long maxWrite = alluxioConf.getBytes(PropertyKey.FUSE_MAXWRITE_BYTES);
      res.add(String.format("-omax_write=%d", maxWrite));
    }
    return res;
  }
}
