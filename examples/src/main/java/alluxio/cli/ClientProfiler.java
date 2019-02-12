package alluxio.cli;

import alluxio.Constants;
import alluxio.cli.profiler.ProfilerClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;
import alluxio.util.JvmHeapDumper;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.IOException;


/**
 * Class to help profile clients with heapdumps. Combine with flamegraphs for full exploitabillty
 */
public class ClientProfiler {

  private static final String DEFAULT_DIR = "/alluxio-profiling";

  enum ClientType {
    alluxio,
    hadoop,
  }

  @Parameter(names = {"-c", "--client"},
      description = "The type of client to profile. Must be one of [alluxio, hadoop].",
      required = true)
  private ClientType mClientType;

  /** The total number of files to create in the filesystem. */
  @Parameter(names = {"-n"}, description = "total number of files to operate on in the filesystem.")
  private int mNumFiles = 5000;

  /** The total amount of data to write across all files in the filesystem. */
  @Parameter(names = {"-s"}, description = "Amount of data to write to use when profiling.")
  private String mDataParam = "128m";
  private long mDataSize;

  /** The number of threads performing concurrent client operations. */
  @Parameter(names = {"-t"}, description = "total threads to perform operations concurrently")
  private long mNumThreads = 1;

  /** The base directory to store files. */
  @Parameter(names = {"-d"}, description = "Base directory to store files")
  private String mDataDir = DEFAULT_DIR;

  @Parameter(names = {"--dump-interval"}, description = "Interval at which to collect heap dumps")
  private String mDumpInterval = "10sec";

  @Parameter(names = {"--dry"}, description = "Perform a dry run by simply printing all operations")
  private boolean mDryRun = false;

  /**
   * New client profiler.
   * @param args command line arguments
   */
  public ClientProfiler(String[] args) {
    parseArgs(args);
  }

  /**
   * Profiles; creating heap dumps.
   */
  public void profile() throws IOException, InterruptedException {
    ProfilerClient.sDryRun = mDryRun;
    ProfilerClient client = ProfilerClient.Factory.create(mClientType.toString());
    System.out.println(System.getProperty("user.dir"));
    mDumpInterval = "1s";
    JvmHeapDumper dumper = new JvmHeapDumper(FormatUtils.parseTimeSize(mDumpInterval), "dumps",
        "dump-" + mClientType.toString());
    client.cleanup(mDataDir);
    dumper.start();
    client.createFiles(mDataDir, mNumFiles, 50, mDataSize / mNumFiles);
    dumper.stopDumps();
    System.exit(0);
  }

  private int parseArgs(String[] args) {
    JCommander jc = new JCommander(this);
    jc.setProgramName("Client Profiler");
    try {
      jc.parse(args);
      mDataSize = FormatUtils.parseSpaceSize(mDataParam);
    } catch (Exception e) {
      System.out.println(e.toString());
      jc.usage();
      System.exit(1);
    }
    return 0;
  }


  /**
   * Run main.
   * @param args arg
   */
  public static void main(String[] args) {
    ClientProfiler profiler = new ClientProfiler(args);
    try {
      profiler.profile();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
