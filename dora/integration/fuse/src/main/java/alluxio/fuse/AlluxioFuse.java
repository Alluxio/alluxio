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

import alluxio.Constants;
import alluxio.ProjectConstants;
import alluxio.RuntimeConstants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.FileSystemOptions;
import alluxio.client.file.options.UfsFileSystemOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.runtime.FailedPreconditionRuntimeException;
import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.fuse.meta.UpdateChecker;
import alluxio.fuse.options.FuseOptions;
import alluxio.heartbeat.FixedIntervalSupplier;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.jnifuse.LibFuse;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.security.user.UserState;
import alluxio.util.CommonUtils;
import alluxio.util.JvmPauseMonitor;
import alluxio.util.network.NetworkAddressUtils;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Main entry point to Alluxio-FUSE.
 */
@ThreadSafe
public class AlluxioFuse {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioFuse.class);
  protected static final CommandLineParser PARSER = new DefaultParser();

  protected static final String MOUNT_POINT_OPTION_NAME = "m";
  protected static final String MOUNT_ROOT_UFS_OPTION_NAME = "u";
  protected static final String MOUNT_OPTIONS_OPTION_NAME = "o";
  protected static final String HELP_OPTION_NAME = "h";
  protected static final String UPDATE_CHECK_OPTION_NAME = "c";

  protected static final Option MOUNT_POINT_OPTION = Option.builder(MOUNT_POINT_OPTION_NAME)
      .hasArg()
      .required(false)
      .longOpt("mount-point")
      .desc("The absolute local filesystem path that standalone Fuse will mount Alluxio path to.")
      .build();
  protected static final Option MOUNT_ROOT_UFS_OPTION
      = Option.builder(MOUNT_ROOT_UFS_OPTION_NAME)
      .hasArg()
      .required(false)
      .longOpt("root-ufs")
      .desc("The storage address of the UFS to mount to the given Fuse mount point. "
          + "All operations against the FUSE mount point "
          + "will be redirected to this storage address. "
          + "(for example, mount storage address `s3://my_bucket/my_folder` "
          + "to local FUSE mount point `/mnt/alluxio-fuse`; "
          + "local operations like `mkdir /mnt/alluxio-fuse/folder` will be translated to "
          + "`mkdir s3://my_bucket/my_folder/folder`)")
      .build();
  protected static final Option MOUNT_OPTIONS = Option.builder(MOUNT_OPTIONS_OPTION_NAME)
      .valueSeparator(',')
      .required(false)
      .hasArgs()
      .desc("Providing mount options separating by comma. "
          + "Mount options includes operating system mount options, "
          + "many FUSE specific mount options (e.g. direct_io,attr_timeout=10s.allow_other), "
          + "Alluxio property key=value pairs, and Alluxio FUSE special mount options "
          + "local_data_cache=<local_cache_directory>,local_cache_size=<size>,"
          + "local_metadata_cache_size=<size>,local_metadata_cache_expire=<timeout>")
      .build();
  protected static final Option UPDATE_CHECK_OPTION = Option.builder(UPDATE_CHECK_OPTION_NAME)
      .required(false)
      .longOpt("update-check")
      .hasArg()
      .desc("Enables or disables the FUSE version update check. "
          + "Disabled by default when connecting to Alluxio system cache or Dora cache. "
          + "Enabled by default when connecting an under storage directly.")
      .build();
  protected static final Option HELP_OPTION = Option.builder(HELP_OPTION_NAME)
      .required(false)
      .desc("Print this help message")
      .build();
  protected static final Options OPTIONS = new Options()
      .addOption(MOUNT_POINT_OPTION)
      .addOption(MOUNT_ROOT_UFS_OPTION)
      .addOption(MOUNT_OPTIONS)
      .addOption(UPDATE_CHECK_OPTION)
      .addOption(HELP_OPTION);

  // prevent instantiation
  protected AlluxioFuse() {
  }

  /**
   * Startup the FUSE process.
   *
   * @param args process arguments
   * @throws ParseException
   */
  public void start(String[] args) throws ParseException {
    CommandLine cli = PARSER.parse(OPTIONS, args);

    if (cli.hasOption(HELP_OPTION_NAME)) {
      final HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp(AlluxioFuse.class.getName(), OPTIONS);
      return;
    }

    LOG.info("Alluxio version: {}-{}", RuntimeConstants.VERSION, ProjectConstants.REVISION);
    setConfigurationFromInput(cli, Configuration.modifiableGlobal());
    AlluxioConfiguration conf = Configuration.global();
    FuseOptions fuseOptions = getFuseOptions(cli, conf);

    FileSystemContext fsContext = FileSystemContext.create(conf);
    if (!fuseOptions.getFileSystemOptions().getUfsFileSystemOptions().isPresent()
        && !fuseOptions.getFileSystemOptions().isDoraCacheEnabled()) {
      // cases other than standalone fuse sdk
      conf = AlluxioFuseUtils.tryLoadingConfigFromMaster(fsContext);
    }

    CommonUtils.PROCESS_TYPE.set(CommonUtils.ProcessType.CLIENT);
    MetricsSystem.startSinks(conf.getString(PropertyKey.METRICS_CONF_FILE));
    if (conf.getBoolean(PropertyKey.FUSE_WEB_ENABLED)) {
      FuseWebServer webServer = new FuseWebServer(
          NetworkAddressUtils.ServiceType.FUSE_WEB.getServiceName(),
          NetworkAddressUtils.getBindAddress(
              NetworkAddressUtils.ServiceType.FUSE_WEB,
              Configuration.global()));
      webServer.start();
    }
    startJvmMonitorProcess();
    ExecutorService executor = null;
    if (fuseOptions.updateCheckEnabled()) {
      executor = Executors.newSingleThreadExecutor();
      executor.submit(new HeartbeatThread(HeartbeatContext.FUSE_UPDATE_CHECK,
          UpdateChecker.create(fuseOptions), () -> new FixedIntervalSupplier(Constants.DAY_MS),
          Configuration.global(), UserState.Factory.create(conf)));
    }
    try (FileSystem fs = FileSystem.Factory.create(fsContext, fuseOptions.getFileSystemOptions())) {
      FuseUmountable fuseFileSystem = createFuseFileSystem(fsContext, fs, fuseOptions);
      if (fuseFileSystem instanceof AlluxioJniFuseFileSystem) {
        AlluxioJniFuseFileSystem jniFuseFileSystem = (AlluxioJniFuseFileSystem) fuseFileSystem;
        setupFuseFileSystem(jniFuseFileSystem);
      }
      launchFuse(fuseFileSystem, fsContext, fuseOptions, true);
    } catch (Throwable t) {
      if (executor != null) {
        executor.shutdown();
      }
      // TODO(lu) FUSE unmount gracefully
      LOG.error("Failed to launch FUSE", t);
      System.exit(-1);
    }
  }

  /**
   * Running this class will mount the file system according to the options passed to this function.
   * The user-space fuse application will stay on the foreground and keep the file system mounted.
   * The user can unmount the file system by gracefully killing (SIGINT) the process.
   *
   * @param args arguments to run the command line
   */
  public static void main(String[] args) throws ParseException {
    AlluxioFuse alluxioFuse = new AlluxioFuse();
    alluxioFuse.start(args);
  }

  /**
   * Create a FuseFileSystem instance.
   *
   * @param fsContext   the context of the file system on which FuseFileSystem based on
   * @param fs          the file system on which FuseFileSystem based on
   * @param fuseOptions the fuse options
   * @return a FuseFileSystem instance
   */
  public FuseUmountable createFuseFileSystem(FileSystemContext fsContext, FileSystem fs,
                                             FuseOptions fuseOptions) {
    AlluxioConfiguration conf = fsContext.getClusterConf();
    validateFuseConfAndOptions(conf, fuseOptions);

    LibFuse.loadLibrary(AlluxioFuseUtils.getLibfuseVersion(conf));

    String mountPoint = conf.getString(PropertyKey.FUSE_MOUNT_POINT);
    Path mountPath = Paths.get(mountPoint);
    if (!Files.exists(mountPath)) {
      LOG.warn("Mount point on local filesystem does not exist, creating {}", mountPoint);
      try {
        Files.createDirectories(mountPath);
      } catch (IOException e) {
        throw new FailedPreconditionRuntimeException("Failed to create mount point");
      }
    }

    if (conf.getBoolean(PropertyKey.FUSE_JNIFUSE_ENABLED)) {
      final AlluxioJniFuseFileSystem fuseFs
          = new AlluxioJniFuseFileSystem(fsContext, fs, fuseOptions);
      return fuseFs;
    } else {
      AlluxioJnrFuseFileSystem fuseFs = new AlluxioJnrFuseFileSystem(fs, conf, fuseOptions);
      return fuseFs;
    }
  }

  /**
   * Set up the FUSE file system {@link AlluxioJniFuseFileSystem} before mounting.
   *
   * @param jniFuseFileSystem the instance of {@link AlluxioJniFuseFileSystem}
   */
  public void setupFuseFileSystem(AlluxioJniFuseFileSystem jniFuseFileSystem) {
    // do nothing by default
  }

  /**
   * Launches Fuse application.
   *
   * @param fuseFs      the fuse file system
   * @param fsContext   file system context for Fuse client to communicate to servers
   * @param fuseOptions Fuse options
   * @param blocking    whether the Fuse application is blocking or not
   * @return the Fuse application handler for future Fuse umount operation
   */
  public static FuseUmountable launchFuse(FuseUmountable fuseFs,
                                          FileSystemContext fsContext, FuseOptions fuseOptions,
                                          boolean blocking) {
    AlluxioConfiguration conf = fsContext.getClusterConf();
    validateFuseConfAndOptions(conf, fuseOptions);

    String mountPoint = conf.getString(PropertyKey.FUSE_MOUNT_POINT);
    Path mountPath = Paths.get(mountPoint);

    final boolean debugEnabled = conf.getBoolean(PropertyKey.FUSE_DEBUG_ENABLED);
    if (conf.getBoolean(PropertyKey.FUSE_JNIFUSE_ENABLED)) {
      FuseSignalHandler fuseSignalHandler = new FuseSignalHandler(fuseFs);
      Signal.handle(new Signal("TERM"), fuseSignalHandler);
      try {
        LOG.info("Mounting AlluxioJniFuseFileSystem: mount point=\"{}\", OPTIONS=\"{}\"",
            mountPoint, String.join(",", fuseOptions.getFuseMountOptions()));
        if (fuseFs instanceof AlluxioJniFuseFileSystem) {
          ((AlluxioJniFuseFileSystem) fuseFs).mount(blocking, debugEnabled,
              fuseOptions.getFuseMountOptions());
        }
        return fuseFs;
      } catch (RuntimeException e) {
        fuseFs.umount(true);
        throw e;
      }
    } else {
      try {
        if (fuseFs instanceof AlluxioJnrFuseFileSystem) {
          ((AlluxioJnrFuseFileSystem) fuseFs).mount(mountPath, blocking, debugEnabled,
              fuseOptions.getFuseMountOptions().stream().map(a -> "-o" + a).toArray(String[]::new));
        }
        return fuseFs;
      } catch (Throwable t) {
        // only try to umount file system when exception occurred.
        // jnr-fuse registers JVM shutdown hook to ensure fs.umount()
        // will be executed when this process is exiting.
        fuseFs.umount(true);
        throw t;
      }
    }
  }

  /**
   * Updates Alluxio configuration according to command line input.
   *
   * @param cli  the command line inputs
   * @param conf the modifiable configuration to update
   */
  protected static void setConfigurationFromInput(CommandLine cli, InstancedConfiguration conf) {
    if (cli.hasOption(MOUNT_POINT_OPTION_NAME)) {
      conf.set(PropertyKey.FUSE_MOUNT_POINT,
          cli.getOptionValue(MOUNT_POINT_OPTION_NAME), Source.RUNTIME);
    }
    if (cli.hasOption(MOUNT_ROOT_UFS_OPTION_NAME)) {
      String ufs = cli.getOptionValue(MOUNT_ROOT_UFS_OPTION_NAME);
      if (ufs.startsWith(Constants.SCHEME)) {
        conf.set(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH, ufs, Source.RUNTIME);
      } else {
        // Disable connections between FUSE and server
        conf.set(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false, Source.RUNTIME);
        conf.set(PropertyKey.USER_UPDATE_FILE_ACCESSTIME_DISABLED, true, Source.RUNTIME);
      }
    }
    if (cli.hasOption(MOUNT_OPTIONS_OPTION_NAME)) {
      List<String> fuseOptions = new ArrayList<>();
      String[] mountOptionsArray = cli.getOptionValues(MOUNT_OPTIONS_OPTION_NAME);
      for (String opt : mountOptionsArray) {
        String trimedOpt = opt.trim();
        if (trimedOpt.isEmpty()) {
          continue;
        }
        String[] optArray = trimedOpt.split("=");
        if (optArray.length == 1) {
          fuseOptions.add(trimedOpt);
          continue;
        }
        String key = optArray[0];
        String value = optArray[1];
        if (PropertyKey.isValid(key)) {
          PropertyKey propertyKey = PropertyKey.fromString(key);
          conf.set(propertyKey, propertyKey.parseValue(value), Source.RUNTIME);
          LOG.info("Set Alluxio property key({}={}) from command line input", key, value);
        } else if (key.equals("fuse")) {
          conf.set(PropertyKey.FUSE_JNIFUSE_LIBFUSE_VERSION,
              PropertyKey.FUSE_JNIFUSE_LIBFUSE_VERSION.parseValue(value), Source.RUNTIME);
          LOG.info("Set libfuse version to {} from command line input", value);
        } else if (key.equals("local_data_cache")) {
          conf.set(PropertyKey.USER_CLIENT_CACHE_ENABLED, true, Source.RUNTIME);
          conf.set(PropertyKey.USER_CLIENT_CACHE_DIRS,
              PropertyKey.USER_CLIENT_CACHE_DIRS.parseValue(value), Source.RUNTIME);
          LOG.info("Set data cache to {} from command line input", value);
        } else if (key.equals("local_data_cache_size")) {
          conf.set(PropertyKey.USER_CLIENT_CACHE_SIZE,
              PropertyKey.USER_CLIENT_CACHE_SIZE.parseValue(value), Source.RUNTIME);
          LOG.info("Set data cache size as {} from command line input", value);
        } else if (key.equals("local_metadata_cache_size")) {
          conf.set(PropertyKey.USER_METADATA_CACHE_MAX_SIZE,
              PropertyKey.USER_METADATA_CACHE_MAX_SIZE.parseValue(value), Source.RUNTIME);
          LOG.info("Set metadata cache size as {} from command line input", value);
        } else if (key.equals("local_metadata_cache_expire")) {
          conf.set(PropertyKey.USER_METADATA_CACHE_EXPIRATION_TIME,
              PropertyKey.USER_METADATA_CACHE_EXPIRATION_TIME.parseValue(value), Source.RUNTIME);
          LOG.info("Set metadata cache expiration time as {} from command line input", value);
        } else {
          fuseOptions.add(trimedOpt);
        }
      }
      if (!fuseOptions.isEmpty()) {
        conf.set(PropertyKey.FUSE_MOUNT_OPTIONS, fuseOptions, Source.RUNTIME);
        LOG.info("Set fuse mount point options as {} from command line input",
            String.join(",", fuseOptions));
      }
    }
    Source metadataCacheSizeSource = conf.getSource(PropertyKey.USER_METADATA_CACHE_MAX_SIZE);
    if (metadataCacheSizeSource == Source.DEFAULT
        || metadataCacheSizeSource == Source.CLUSTER_DEFAULT) {
      conf.set(PropertyKey.USER_METADATA_CACHE_MAX_SIZE, 20000, Source.RUNTIME);
      LOG.info("Set default metadata cache size to 20,000 entries "
          + "with around 40MB memory consumption for FUSE");
    }
  }

  protected static FuseOptions getFuseOptions(CommandLine cli, AlluxioConfiguration conf) {
    boolean updateCheckEnabled = false;
    if (cli.hasOption(UPDATE_CHECK_OPTION_NAME)) {
      updateCheckEnabled = Boolean.parseBoolean(cli.getOptionValue(UPDATE_CHECK_OPTION_NAME));
    } else if (!conf.getBoolean(PropertyKey.DORA_ENABLED)
        && cli.hasOption(MOUNT_ROOT_UFS_OPTION_NAME)) {
      // Standalone FUSE SDK without distributed cache
      updateCheckEnabled = true;
    }
    if (cli.hasOption(MOUNT_ROOT_UFS_OPTION_NAME)
        && !cli.getOptionValue(MOUNT_ROOT_UFS_OPTION_NAME).startsWith(Constants.SCHEME)) {
      final UfsFileSystemOptions ufsFileSystemOptions =
          new UfsFileSystemOptions(cli.getOptionValue(MOUNT_ROOT_UFS_OPTION_NAME));
      final FileSystemOptions fileSystemOptions = FileSystemOptions.Builder.fromConf(conf)
          .setUfsFileSystemOptions(ufsFileSystemOptions)
          .build();
      return FuseOptions.create(conf, fileSystemOptions, updateCheckEnabled);
    } else {
      return FuseOptions.create(conf, updateCheckEnabled);
    }
  }

  protected static void validateFuseConfAndOptions(AlluxioConfiguration conf, FuseOptions options) {
    String mountPoint = conf.getString(PropertyKey.FUSE_MOUNT_POINT);
    if (mountPoint.isEmpty()) {
      throw new InvalidArgumentRuntimeException(
          String.format("%s should be set and should not be empty",
              PropertyKey.FUSE_MOUNT_POINT.getName()));
    }
    if (!options.getFileSystemOptions().getUfsFileSystemOptions().isPresent()
        && conf.getString(PropertyKey.FUSE_MOUNT_ALLUXIO_PATH).isEmpty()) {
      throw new InvalidArgumentRuntimeException(
          String.format("%s should be set and should not be empty",
              PropertyKey.FUSE_MOUNT_ALLUXIO_PATH.getName()));
    }
    if (Files.isRegularFile(Paths.get(mountPoint))) {
      LOG.error("Mount point {} is not a directory but a file", mountPoint);
      throw new InvalidArgumentRuntimeException("Failed to launch fuse, mount point is a file");
    }
  }

  /**
   * Starts jvm monitor process, to monitor jvm.
   */
  protected static void startJvmMonitorProcess() {
    if (Configuration.getBoolean(PropertyKey.STANDALONE_FUSE_JVM_MONITOR_ENABLED)) {
      JvmPauseMonitor jvmPauseMonitor = new JvmPauseMonitor(
          Configuration.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS),
          Configuration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS),
          Configuration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS));
      jvmPauseMonitor.start();
      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.TOTAL_EXTRA_TIME.getName()),
          jvmPauseMonitor::getTotalExtraTime);
      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.INFO_TIME_EXCEEDED.getName()),
          jvmPauseMonitor::getInfoTimeExceeded);
      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.WARN_TIME_EXCEEDED.getName()),
          jvmPauseMonitor::getWarnTimeExceeded);
    }
  }
}
