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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.ClientOptions;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
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
    // TODO: add it back when supporting raw table
    //CREATE_TABLE,
    // TODO: add more op types to test
  }

  /**
   * The client thread class. Each thread hold a Tachyon Client and keep requesting to Master.
   */
  static class ClientThread extends Thread {
    /** Which type of operation this thread should do. */
    private final ClientOpType mOpType;
    /** The working directory of this thread on Tachyon. */
    private final String mWorkDir;

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
      try {
        // This infinity loop will be broken if something crashes or fails. This is
        // expected since we are testing the crash scenario.
        while (true) {
          TachyonURI testURI = new TachyonURI(mWorkDir + mSuccessNum);
          if (ClientOpType.CREATE_FILE == mOpType) {
            sTfs.getOutStream(testURI, sClientOptions).close();
          } else if (ClientOpType.CREATE_DELETE_FILE == mOpType) {
            sTfs.getOutStream(testURI, sClientOptions).close();
            sTfs.delete(sTfs.open(testURI));
          } else if (ClientOpType.CREATE_RENAME_FILE == mOpType) {
            sTfs.getOutStream(testURI, sClientOptions).close();
            if (!sTfs.rename(sTfs.open(testURI), new TachyonURI(testURI + "-rename"))) {
              break;
            }
          }
          //else if (ClientOpType.CREATE_TABLE == mOpType) {
          //  if (mTfs.createRawTable(new TachyonURI(mWorkDir + mSuccessNum), 1) == -1) {
          //    break;
          //  }
          //}
          mSuccessNum ++;
          CommonUtils.sleepMs(null, 100);
        }
      }  catch (Exception e) {
        // Something crashed. Stop the thread.
      }
    }
  }

  // The two Exit Codes are used to tell script if the test runs well.
  private static final int EXIT_FAILED = 1;
  private static final int EXIT_SUCCESS = 0;
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static ClientOptions sClientOptions = null;
  private static List<ClientThread> sClientThreadList = null;
  private static int sCreateDeleteClientNum;
  private static int sCreateFileClientNum;
  private static int sCreateRenameClientNum;
  private static String sTestDir;
  /** The Tachyon Client. This can be shared by all the threads. */
  private static TachyonFileSystem sTfs = null;

  private static boolean checkStatus() throws Exception {
    // Connect to Master and check if all the test operations are reproduced by Master successfully.
    for (ClientThread clientThread : sClientThreadList) {
      ClientOpType opType = clientThread.getOpType();
      String workDir = clientThread.getWorkDir();
      for (int s = 0; s < clientThread.getSuccessNum(); s ++) {
        TachyonURI checkURI = new TachyonURI(workDir + s);
        if (ClientOpType.CREATE_FILE == opType) {
          try {
            sTfs.open(checkURI);
          } catch (IOException ioe) {
            // File not exist. This is unexpected for CREATE_FILE.
            return false;
          }
        } else if (ClientOpType.CREATE_DELETE_FILE == opType) {
          try {
            sTfs.open(checkURI);
          } catch (IOException ioe) {
            // File not exist. This is expected for CREATE_DELETE_FILE.
            continue;
          }
          return false;
        } else if (ClientOpType.CREATE_RENAME_FILE == opType) {
          try {
            sTfs.open(new TachyonURI(checkURI + "-rename"));
          } catch (IOException ioe) {
            // File not exist. This is unexpected for CREATE_RENAME_FILE.
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

  public static void main(String[] args) {
    // Parse the input args.
    if (!parseInputArgs(args)) {
      printUsage();
      System.exit(EXIT_FAILED);
    }

    // Set NO_STORE and NO_PERSIST so that this test can work without TachyonWorker.
    sClientOptions = new ClientOptions.Builder(new TachyonConf())
        .setStorageTypes(TachyonStorageType.NO_STORE, UnderStorageType.NO_PERSIST).build();
    sClientThreadList = new ArrayList<ClientThread>();
    sTfs = TachyonFileSystem.get();
    try {
      sTfs.delete(sTfs.open(new TachyonURI(sTestDir)));
    } catch (IOException ioe) {
      // Test Directory not exist
    }

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

    // Launch all the client threads and wait for them. If Master crashes, all the threads will
    // stop at a certain time.
    for (Thread thread : sClientThreadList) {
      thread.start();
    }
    for (Thread thread : sClientThreadList) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        LOG.error("Error when waiting thread", e);
      }
    }

    // Restart Tachyon Master.
    String restartMasterCommand = new TachyonConf().get(Constants.TACHYON_HOME)
        + "/bin/tachyon-start.sh master";
    try {
      Runtime.getRuntime().exec(restartMasterCommand).waitFor();
    } catch (Exception e) {
      LOG.error("Error when restarting Master", e);
    }
    // Wait for Master restart.
    CommonUtils.sleepMs(null, 1000);
    boolean isRestart = false;
    while (!isRestart) {
      try {
        // ping Master
        sTfs.getInfo(new TachyonFile(0));
        isRestart = true;
      } catch (Exception e) {
        // Master has not started.
      }
      CommonUtils.sleepMs(null, 1000);
    }

    // Check status and print pass info.
    try {
      if (!checkStatus()) {
        Utils.printPassInfo(false);
        System.exit(EXIT_FAILED);
      }
      Utils.printPassInfo(true);
      System.exit(EXIT_SUCCESS);
    } catch (Exception e) {
      LOG.error("Failed to check status", e);
    }

    System.exit(EXIT_FAILED);
  }

  /**
   * Parse the input args with a command line format, using
   * <code>org.apache.commons.cli.CommandLineParser</code>.
   * @param args the input args
   * @return true if parse successfully, false otherwise
   */
  private static boolean parseInputArgs(String[] args) {
    Options options = new Options();
    options.addOption("testDir", true, "Test Directory on Tachyon");
    options.addOption("creates", true, "create Threads Num");
    options.addOption("deletes", true, "create/delete Threads Num");
    options.addOption("renames", true, "create/rename Threads Num");
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      LOG.error("Failed to parse input args", e);
      return false;
    }
    sTestDir = cmd.getOptionValue("testDir", "/default_tests_files");
    sCreateFileClientNum = Integer.parseInt(cmd.getOptionValue("creates", "2"));
    sCreateDeleteClientNum = Integer.parseInt(cmd.getOptionValue("deletes", "2"));
    sCreateRenameClientNum = Integer.parseInt(cmd.getOptionValue("renames", "2"));
    return true;
  }

  private static void printUsage() {
    System.out.println("Usage: java -cp tachyon-" + Version.VERSION + "-jar-with-dependencies.jar "
        + "tachyon.examples.JournalCrashTest [-options]");
    System.out.println("where options include:");
    System.out.println("\t-testDir <Test Directory on Tachyon>");
    System.out.println("\t-creates <Number of Client Threads to request create operations>");
    System.out.println("\t-deletes <Number of Client Threads to request create/delete operations>");
    System.out.println("\t-renames <Number of Client Threads to request create/rename operations>");
  }
}
