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

package alluxio.cli;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.util.CommonUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to perform Journal crash test. The clients issue commands to the master, and the master
 * generates journal events. Check if the master can generate and reproduce the journal correctly.
 */
public final class JournalCrashTest {

  private JournalCrashTest() {} // prevent instantiation

  /**
   * The operation types to test.
   */
  enum ClientOpType {
    /**
     * Keep creating empty file.
     */
    CREATE_FILE,
    /**
     * Keep creating and deleting file.
     */
    CREATE_DELETE_FILE,
    /**
     * Keep creating and renaming file.
     */
    CREATE_RENAME_FILE,
  }

  /**
   * The client thread class. Each thread holds an Alluxio Client and keeps requesting to Master.
   */
  static class ClientThread extends Thread {
    /** Which type of operation this thread should do. */
    private final ClientOpType mOpType;
    /** The working directory of this thread on Alluxio. */
    private final String mWorkDir;

    /** Used for supervisor to stop this thread. */
    private boolean mIsStopped = false;
    /** The number of successfully operations. */
    private int mSuccessNum = 0;

    /**
     * @param workDir the working directory for this thread on Alluxio
     * @param opType the type of operation this thread should do
     */
    public ClientThread(String workDir, ClientOpType opType) {
      mOpType = opType;
      mWorkDir = workDir;
    }

    /**
     * @return the type of operation this thread should do
     */
    public ClientOpType getOpType() {
      return mOpType;
    }

    /**
     * @return the number of successfully operations
     */
    public int getSuccessNum() {
      return mSuccessNum;
    }

    /**
     * @return the working directory of this thread on Alluxio
     */
    public String getWorkDir() {
      return mWorkDir;
    }

    /**
     * Keeps requesting to Master until something crashes or fail to create. Records how many
     * operations are performed successfully.
     */
    @Override
    public void run() {
      // This infinity loop will be broken when the master is crashed and the client needs to stop.
      while (true) {
        synchronized (this) {
          if (mIsStopped) {
            break;
          }
        }
        try {
          AlluxioURI testURI = new AlluxioURI(mWorkDir + mSuccessNum);
          if (ClientOpType.CREATE_FILE == mOpType) {
            sFileSystem.createFile(testURI, sCreateFileOptions).close();
          } else if (ClientOpType.CREATE_DELETE_FILE == mOpType) {
            try {
              sFileSystem.createFile(testURI, sCreateFileOptions).close();
            } catch (AlluxioException e) {
              // If file already exists, ignore it.
              if (!(e instanceof FileAlreadyExistsException)) {
                throw e;
              }
            }
            sFileSystem.delete(testURI);
          } else if (ClientOpType.CREATE_RENAME_FILE == mOpType) {
            try {
              sFileSystem.createFile(testURI, sCreateFileOptions).close();
            } catch (AlluxioException e) {
              // If file already exists, ignore it.
              if (!(e instanceof FileAlreadyExistsException)) {
                throw e;
              }
            }
            sFileSystem.rename(testURI, new AlluxioURI(testURI + "-rename"));
          }
        } catch (Exception e) {
          // Since master may crash/restart for several times, so this exception is expected.
          // Ignore the exception and still keep requesting to master.
          continue;
        }
        mSuccessNum++;
        CommonUtils.sleepMs(100);
      }
    }

    /**
     * @param isStopped signal from supervisor to stop this thread
     */
    public synchronized void setIsStopped(boolean isStopped) {
      mIsStopped = isStopped;
    }
  }

  // The two Exit Codes are used to tell script if the test runs well.
  private static final int EXIT_FAILED = 1;
  private static final int EXIT_SUCCESS = 0;
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static CreateFileOptions sCreateFileOptions = null;
  private static List<ClientThread> sClientThreadList = null;
  private static int sCreateDeleteClientNum;
  private static int sCreateFileClientNum;
  private static int sCreateRenameClientNum;
  /** The maximum time a master should ever be alive. */
  private static long sMaxAliveTimeMs;
  private static String sTestDir;
  /** The Alluxio Client. This can be shared by all the threads. */
  private static FileSystem sFileSystem = null;
  /** The total time to run this test. */
  private static long sTotalTimeMs;

  private static boolean checkStatus() throws Exception {
    // Connect to Master and check if all the test operations are reproduced by Master successfully.
    for (ClientThread clientThread : sClientThreadList) {
      ClientOpType opType = clientThread.getOpType();
      String workDir = clientThread.getWorkDir();
      int successNum = clientThread.getSuccessNum();
      LOG.info("Expected Status: OpType[{}] WorkDir[{}] SuccessNum[{}].",
          opType, workDir, successNum);
      for (int s = 0; s < successNum; s++) {
        AlluxioURI checkURI = new AlluxioURI(workDir + s);
        if (ClientOpType.CREATE_FILE == opType) {
          if (!sFileSystem.exists(checkURI)) {
            // File not exist. This is unexpected for CREATE_FILE.
            LOG.error("File not exist for create test. Check failed! File: {}", checkURI);
            return false;
          }
        } else if (ClientOpType.CREATE_DELETE_FILE == opType) {
          if (sFileSystem.exists(checkURI)) {
            LOG.error("File exists for create/delete test. Check failed! File: {}", checkURI);
            return false;
          }
        } else if (ClientOpType.CREATE_RENAME_FILE == opType) {
          if (!sFileSystem.exists(new AlluxioURI(checkURI + "-rename"))) {
            // File not exist. This is unexpected for CREATE_FILE.
            LOG.error("File not exist for create/rename test. Check failed! File: {}-rename",
                checkURI);
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Kills Alluxio Master by 'kill -9' command.
   */
  private static void killMaster() {
    String[] killMasterCommand = new String[]{"/usr/bin/env", "bash", "-c",
        "for pid in `ps -Aww -o pid,command | grep -i \"[j]ava\" | grep "
            + "\"alluxio.master.AlluxioMaster\" | awk '{print $1}'`; do kill -9 \"$pid\"; done"};
    try {
      Runtime.getRuntime().exec(killMasterCommand).waitFor();
      CommonUtils.sleepMs(LOG, 1000);
    } catch (Exception e) {
      LOG.error("Error when killing Master", e);
    }
  }

  /**
   * Runs the crash test.
   *
   * @param args no arguments
   */
  public static void main(String[] args) {
    // Parse the input args.
    if (!parseInputArgs(args)) {
      System.exit(EXIT_FAILED);
    }

    System.out.println("Stop the current Alluxio cluster...");
    stopCluster();

    // Set NO_STORE and NO_PERSIST so that this test can work without AlluxioWorker.
    sCreateFileOptions = CreateFileOptions.defaults().setWriteType(WriteType.NONE);
    // Set the max retry to avoid long pending for client disconnect.
    if (System.getProperty(Constants.MASTER_RETRY_COUNT) == null) {
      System.setProperty(Constants.MASTER_RETRY_COUNT, "10");
    }

    System.out.println("Start Journal Crash Test...");
    long startTimeMs = System.currentTimeMillis();
    boolean ret = true;
    startMaster();

    int rounds = 0;
    while (System.currentTimeMillis() - startTimeMs < sTotalTimeMs) {
      rounds++;
      long aliveTimeMs = (long) (Math.random() * sMaxAliveTimeMs) + 100;
      LOG.info("Round {}: Planning Master Alive Time {}ms.", rounds, aliveTimeMs);

      System.out.println("Round " + rounds + " : Launch Clients...");
      sFileSystem = FileSystem.Factory.get();
      try {
        sFileSystem.delete(new AlluxioURI(sTestDir));
      } catch (Exception e) {
        // Test Directory not exist
      }

      // Launch all the client threads.
      setupClientThreads();
      for (Thread thread : sClientThreadList) {
        thread.start();
      }

      CommonUtils.sleepMs(LOG, aliveTimeMs);
      System.out.println("Round " + rounds + " : Crash Master...");
      killMaster();
      for (ClientThread clientThread : sClientThreadList) {
        clientThread.setIsStopped(true);
      }
      for (Thread thread : sClientThreadList) {
        try {
          thread.join();
        } catch (InterruptedException e) {
          LOG.error("Error when waiting thread", e);
        }
      }

      System.out.println("Round " + rounds + " : Check Status...");
      startMaster();
      boolean checkSuccess = false;
      try {
        checkSuccess = checkStatus();
      } catch (Exception e) {
        LOG.error("Failed to check status", e);
      }
      CliUtils.printPassInfo(checkSuccess);
      ret &= checkSuccess;
    }

    stopCluster();
    System.exit(ret ? EXIT_SUCCESS : EXIT_FAILED);
  }

  /**
   * Parses the input args with a command line format, using
   * {@link org.apache.commons.cli.CommandLineParser}. This method handles printing help information
   * if parsing fails or --help is specified.
   *
   * @param args the input args
   * @return true if parsing succeeded and --help wasn't specified, false otherwise
   */
  private static boolean parseInputArgs(String[] args) {
    Options options = new Options();
    options.addOption("help", false, "Show help for this test");
    options.addOption("maxAlive", true,
        "The maximum time a master should ever be alive during the test, in seconds");
    options.addOption("totalTime", true, "The total time to run this test, in seconds."
        + " This value should be greater than [maxAlive]");
    options.addOption("creates", true, "Number of Client Threads to request create operations");
    options.addOption("deletes", true,
        "Number of Client Threads to request create/delete operations");
    options.addOption("renames", true,
        "Number of Client Threads to request create/rename operations");
    options.addOption("testDir", true, "Test Directory on Alluxio");
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    boolean ret = true;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      LOG.error("Failed to parse input args", e);
      ret = false;
    }
    if (ret && !cmd.hasOption("help")) {
      sMaxAliveTimeMs = 1000 * Long.parseLong(cmd.getOptionValue("maxAlive", "5"));
      sTotalTimeMs = 1000 * Long.parseLong(cmd.getOptionValue("totalTime", "20"));
      sCreateFileClientNum = Integer.parseInt(cmd.getOptionValue("creates", "2"));
      sCreateDeleteClientNum = Integer.parseInt(cmd.getOptionValue("deletes", "2"));
      sCreateRenameClientNum = Integer.parseInt(cmd.getOptionValue("renames", "2"));
      sTestDir = cmd.getOptionValue("testDir", "/default_tests_files");
    } else {
      ret = false;
      new HelpFormatter().printHelp(String.format("java -cp %s %s",
          RuntimeConstants.ALLUXIO_JAR, JournalCrashTest.class.getCanonicalName()),
          "Test the Master Journal System in a crash scenario", options,
          "e.g. options '-maxAlive 5 -totalTime 20 -creates 2 -deletes 2 -renames 2'"
          + "will launch total 6 clients connecting to the Master and the Master"
          + "will crash randomly with the max alive time 5 seconds.", true);
    }
    return ret;
  }

  /**
   * Setups all the client threads.
   */
  private static void setupClientThreads() {
    sClientThreadList = new ArrayList<>();
    for (int i = 0; i < sCreateFileClientNum; i++) {
      ClientThread thread = new ClientThread(sTestDir + "/createFile" + i + "/",
          ClientOpType.CREATE_FILE);
      sClientThreadList.add(thread);
    }
    for (int i = 0; i < sCreateDeleteClientNum; i++) {
      ClientThread thread = new ClientThread(sTestDir + "/createDelete" + i + "/",
          ClientOpType.CREATE_DELETE_FILE);
      sClientThreadList.add(thread);
    }
    for (int i = 0; i < sCreateRenameClientNum; i++) {
      ClientThread thread = new ClientThread(sTestDir + "/createRename" + i + "/",
          ClientOpType.CREATE_RENAME_FILE);
      sClientThreadList.add(thread);
    }
  }

  /**
   * Starts Alluxio Master by executing the launch script.
   */
  private static void startMaster() {
    String startMasterCommand = Configuration.get(Constants.HOME)
        + "/bin/alluxio-start.sh master";
    try {
      Runtime.getRuntime().exec(startMasterCommand).waitFor();
      CommonUtils.sleepMs(LOG, 1000);
    } catch (Exception e) {
      LOG.error("Error when starting Master", e);
    }
  }

  /**
   * Stops the current Alluxio cluster. This is used for preparation and clean up.
   * To crash the Master, use {@link #killMaster()}.
   */
  private static void stopCluster() {
    String stopClusterCommand = Configuration.get(Constants.HOME)
        + "/bin/alluxio-stop.sh all";
    try {
      Runtime.getRuntime().exec(stopClusterCommand).waitFor();
      CommonUtils.sleepMs(LOG, 1000);
    } catch (Exception e) {
      LOG.error("Error when stop Alluxio cluster", e);
    }
  }
}
