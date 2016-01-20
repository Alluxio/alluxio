/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.examples;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.WriteType;
import tachyon.client.file.FileSystem;
import tachyon.client.file.options.CreateFileOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.exception.TachyonExceptionType;
import tachyon.util.CommonUtils;

/**
 * Class to perform Journal crash test. The clients issue commands to the master, and the master
 * generates journal events. Check if the master can generate and reproduce the journal correctly.
 */
public class JournalCrashTest {

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
    /**
     * Keep creating empty raw table.
     */
    CREATE_TABLE,
  }

  /**
   * The client thread class. Each thread hold a Tachyon Client and keep requesting to Master.
   */
  static class ClientThread extends Thread {
    /** Which type of operation this thread should do. */
    private final ClientOpType mOpType;
    /** The working directory of this thread on Tachyon. */
    private final String mWorkDir;

    /** Used for supervisor to stop this thread. */
    private boolean mIsStopped = false;
    /** The number of successfully operations. */
    private int mSuccessNum = 0;

    public ClientThread(String workDir, ClientOpType opType) {
      mOpType = opType;
      mWorkDir = workDir;
    }

    public ClientOpType getOpType() {
      return mOpType;
    }

    public int getSuccessNum() {
      return mSuccessNum;
    }

    public String getWorkDir() {
      return mWorkDir;
    }

    /**
     * Keep requesting to Master until something crashes or fail to create. Record how many
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
          TachyonURI testURI = new TachyonURI(mWorkDir + mSuccessNum);
          if (ClientOpType.CREATE_FILE == mOpType) {
            sTfs.createFile(testURI, sCreateFileOptions).close();
          } else if (ClientOpType.CREATE_DELETE_FILE == mOpType) {
            try {
              sTfs.createFile(testURI, sCreateFileOptions).close();
            } catch (TachyonException e) {
              // If file already exists, ignore it.
              if (e.getType() != TachyonExceptionType.FILE_ALREADY_EXISTS) {
                throw e;
              }
            } catch (Exception e) {
              throw e;
            }
            sTfs.delete(testURI);
          } else if (ClientOpType.CREATE_RENAME_FILE == mOpType) {
            try {
              sTfs.createFile(testURI, sCreateFileOptions).close();
            } catch (TachyonException e) {
              // If file already exists, ignore it.
              if (e.getType() != TachyonExceptionType.FILE_ALREADY_EXISTS) {
                throw e;
              }
            } catch (Exception e) {
              throw e;
            }
            sTfs.rename(testURI, new TachyonURI(testURI + "-rename"));
          }
        } catch (Exception e) {
          // Since master may crash/restart for several times, so this exception is expected.
          // Ignore the exception and still keep requesting to master.
          continue;
        }
        mSuccessNum ++;
        CommonUtils.sleepMs(100);
      }
    }

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
  /** The Tachyon Client. This can be shared by all the threads. */
  private static FileSystem sTfs = null;
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
      for (int s = 0; s < successNum; s ++) {
        TachyonURI checkURI = new TachyonURI(workDir + s);
        if (ClientOpType.CREATE_FILE == opType) {
          if (!sTfs.exists(checkURI)) {
            // File not exist. This is unexpected for CREATE_FILE.
            LOG.error("File not exist for create test. Check failed! File: {}", checkURI);
            return false;
          }
        } else if (ClientOpType.CREATE_DELETE_FILE == opType) {
          if (sTfs.exists(checkURI)) {
            LOG.error("File exists for create/delete test. Check failed! File: {}", checkURI);
            return false;
          }
        } else if (ClientOpType.CREATE_RENAME_FILE == opType) {
          if (!sTfs.exists(new TachyonURI(checkURI + "-rename"))) {
            // File not exist. This is unexpected for CREATE_FILE.
            LOG.error("File not exist for create/rename test. Check failed! File: {}-rename",
                checkURI);
            return false;
          }
        }
        //else if (ClientOpType.CREATE_TABLE == opType) {
        //  if (tfs.getRawTable(new TachyonURI(workDir + s)).getId() == -1) {
        //    tfs.close();
        //    return false;
        //  }
        //}
      }
    }
    return true;
  }

  /**
   * Kill Tachyon Master by 'kill -9' command.
   */
  private static void killMaster() {
    String[] killMasterCommand = new String[]{"/usr/bin/env", "bash", "-c",
        "for pid in `ps -Aww -o pid,command | grep -i \"[j]ava\" | grep "
            + "\"tachyon.master.TachyonMaster\" | awk '{print $1}'`; do kill -9 \"$pid\"; done"};
    try {
      Runtime.getRuntime().exec(killMasterCommand).waitFor();
      CommonUtils.sleepMs(LOG, 1000);
    } catch (Exception e) {
      LOG.error("Error when killing Master", e);
    }
  }

  public static void main(String[] args) {
    // Parse the input args.
    if (!parseInputArgs(args)) {
      System.exit(EXIT_FAILED);
    }

    System.out.println("Stop the current Tachyon cluster...");
    stopCluster();

    // Set NO_STORE and NO_PERSIST so that this test can work without TachyonWorker.
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
      rounds ++;
      long aliveTimeMs = (long)(Math.random() * sMaxAliveTimeMs) + 100;
      LOG.info("Round {}: Planning Master Alive Time {}ms.", rounds, aliveTimeMs);

      System.out.println("Round " + rounds + " : Launch Clients...");
      sTfs = FileSystem.Factory.get();
      try {
        sTfs.delete(new TachyonURI(sTestDir));
      } catch (Exception ioe) {
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
      Utils.printPassInfo(checkSuccess);
      ret &= checkSuccess;
    }

    stopCluster();
    System.exit(ret ? EXIT_SUCCESS : EXIT_FAILED);
  }

  /**
   * Parse the input args with a command line format, using
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
    options.addOption("testDir", true, "Test Directory on Tachyon");
    CommandLineParser parser = new BasicParser();
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
      new HelpFormatter().printHelp("java -cp tachyon-" + Version.VERSION
          + "-jar-with-dependencies.jar tachyon.examples.JournalCrashTest",
          "Test the Master Journal System in a crash scenario", options,
          "e.g. options '-maxAlive 5 -totalTime 20 -creates 2 -deletes 2 -renames 2'"
          + "will launch total 6 clients connecting to the Master and the Master"
          + "will crash randomly with the max alive time 5 seconds.", true);
    }
    return ret;
  }

  /**
   * Setup all the client threads.
   */
  private static void setupClientThreads() {
    sClientThreadList = new ArrayList<ClientThread>();
    for (int i = 0; i < sCreateFileClientNum; i ++) {
      ClientThread thread = new ClientThread(sTestDir + "/createFile" + i + "/",
          ClientOpType.CREATE_FILE);
      sClientThreadList.add(thread);
    }
    for (int i = 0; i < sCreateDeleteClientNum; i ++) {
      ClientThread thread = new ClientThread(sTestDir + "/createDelete" + i + "/",
          ClientOpType.CREATE_DELETE_FILE);
      sClientThreadList.add(thread);
    }
    for (int i = 0; i < sCreateRenameClientNum; i ++) {
      ClientThread thread = new ClientThread(sTestDir + "/createRename" + i + "/",
          ClientOpType.CREATE_RENAME_FILE);
      sClientThreadList.add(thread);
    }
  }

  /**
   * Start Tachyon Master by executing the launch script.
   */
  private static void startMaster() {
    String startMasterCommand = new TachyonConf().get(Constants.TACHYON_HOME)
        + "/bin/tachyon-start.sh master";
    try {
      Runtime.getRuntime().exec(startMasterCommand).waitFor();
      CommonUtils.sleepMs(LOG, 1000);
    } catch (Exception e) {
      LOG.error("Error when starting Master", e);
    }
  }

  /**
   * Stop the current Tachyon cluster. This is used for preparation and clean up.
   * To crash the Master, use {@link #killMaster()}.
   */
  private static void stopCluster() {
    String stopClusterCommand = new TachyonConf().get(Constants.TACHYON_HOME)
        + "/bin/tachyon-stop.sh all";
    try {
      Runtime.getRuntime().exec(stopClusterCommand).waitFor();
      CommonUtils.sleepMs(LOG, 1000);
    } catch (Exception e) {
      LOG.error("Error when stop Tachyon cluster", e);
    }
  }
}
