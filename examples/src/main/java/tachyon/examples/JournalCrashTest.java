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
import tachyon.client.TachyonFS;
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
    /** The Tachyon Client hold by this thread. */
    // TODO: use TachyonFileSystem instead of the deprecated TachyonFS
    private final TachyonFS mTfs;
    /** The working directory of this thread on Tachyon. */
    private final String mWorkDir;

    /** The number of successfully operations. */
    private int mSuccessNum = 0;

    public ClientThread(TachyonFS tfs, String workDir, ClientOpType opType) {
      mOpType = opType;
      mTfs = tfs;
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
          if (ClientOpType.CREATE_FILE == mOpType) {
            if (mTfs.createFile(new TachyonURI(mWorkDir + mSuccessNum)) == -1) {
              break;
            }
          } else if (ClientOpType.CREATE_DELETE_FILE == mOpType) {
            int fid = mTfs.createFile(new TachyonURI(mWorkDir + mSuccessNum));
            if (fid == -1) {
              break;
            }
            if (!mTfs.delete(fid, false)) {
              break;
            }
          } else if (ClientOpType.CREATE_RENAME_FILE == mOpType) {
            int fid = mTfs.createFile(new TachyonURI(mWorkDir + mSuccessNum));
            if (fid == -1) {
              break;
            }
            if (!mTfs.rename(fid, new TachyonURI(mWorkDir + mSuccessNum + "-rename"))) {
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
      } finally {
        try {
          mTfs.close();
        } catch (IOException e) {
          LOG.error("Error when stop client.", e);
        }
      }
    }
  }

  // The two Exit Codes are used to tell script if the test runs well.
  private static final int EXIT_FAILED = 1;
  private static final int EXIT_SUCCESS = 0;
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static int sClientNum = 10;
  private static TachyonURI sMasterAddress = null;
  private static String sTestDir = null;
  private static List<ClientThread> sClientThreadList = null;

  private static boolean checkStatus() throws Exception {
    // Launch a Tachyon Client and connect to Master. Check if all the test operations are
    // reproduced by Master successfully.
    TachyonFS tfs = TachyonFS.get(sMasterAddress, new TachyonConf());
    for (ClientThread clientThread : sClientThreadList) {
      ClientOpType opType = clientThread.getOpType();
      String workDir = clientThread.getWorkDir();
      for (int s = 0; s < clientThread.getSuccessNum(); s ++) {
        if (ClientOpType.CREATE_FILE == opType) {
          if (tfs.getFileId(new TachyonURI(workDir + s)) == -1) {
            tfs.close();
            return false;
          }
        } else if (ClientOpType.CREATE_DELETE_FILE == opType) {
          if (tfs.getFileId(new TachyonURI(workDir + s)) != -1) {
            tfs.close();
            return false;
          }
        } else if (ClientOpType.CREATE_RENAME_FILE == opType) {
          if (tfs.getFileId(new TachyonURI(workDir + s + "-rename")) == -1) {
            tfs.close();
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
    tfs.close();
    return true;
  }

  public static void main(String[] args) {
    if (args.length < 4) {
      System.out.println("java -cp tachyon-" + Version.VERSION + "-jar-with-dependencies.jar "
          + "tachyon.examples.JournalCrashTest "
          + "<TachyonMasterAddress> <TestTachyonDir> [-options]");
      System.exit(EXIT_FAILED);
    }

    sMasterAddress = new TachyonURI(args[0]);
    sTestDir = args[1];

    // Parse the input args.
    CommandLine cmd = parseInputArgs(args);
    if (cmd != null) {
      try {
        sClientNum = Integer.parseInt(cmd.getOptionValue("cn"));
      } catch (NumberFormatException e) {
        LOG.warn("Error clients number. Use the default value 10.");
      }
      // TODO: add more configurable settings for this test
    }

    sClientThreadList = new ArrayList<ClientThread>(sClientNum);

    // Currently, half of the threads to create file and others to create table.
    // TODO: this should be reconsidered when supporting more operations
    int createFileClients = sClientNum;
    for (int f = 0; f < createFileClients; f ++) {
      ClientThread thread = new ClientThread(TachyonFS.get(sMasterAddress, new TachyonConf()),
          sTestDir + "/createFile" + f + "/", ClientOpType.CREATE_FILE);
      sClientThreadList.add(thread);
    }
    //int createTableClients = sClientNum - createFileClients;
    //for (int t = 0; t < createTableClients; t ++) {
    //  ClientThread thread = new ClientThread(TachyonFS.get(sMasterAddress, new TachyonConf()),
    //      sTestDir + "/createTable" + t + "/", ClientOpType.CREATE_TABLE);
    //  sClientThreads.add(thread);
    //  sClientThreadList.add(new Thread(thread));
    //}

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

    // Wait for Master restart.
    CommonUtils.sleepMs(null, 1000);
    TachyonFS waitMasterTfs = TachyonFS.get(sMasterAddress, new TachyonConf());
    while (!waitMasterTfs.isConnected()) {
      try {
        // ping Master
        waitMasterTfs.getFile(0, false);
      } catch (IOException e) {
        // Master has not started.
      }
      CommonUtils.sleepMs(null, 1000);
    }
    try {
      waitMasterTfs.close();
    } catch (IOException e) {
      LOG.error("Error when stop client.", e);
    }

    // Check status and print pass info.
    try {
      if (!checkStatus()) {
        Utils.printPassInfo(false);
        System.exit(EXIT_FAILED);
      }
      Utils.printPassInfo(true);
    } catch (Exception e) {
      LOG.error("Failed to check status", e);
    }

    System.exit(EXIT_SUCCESS);
  }

  /**
   * Parse the input args with a command line format, using
   * <code>org.apache.commons.cli.CommandLineParser</code>.
   * @param args the input args
   * @return the parsed command line
   */
  private static CommandLine parseInputArgs(String[] args) {
    CommandLine ret = null;
    Options options = new Options();
    options.addOption("cn", true, "Clients number");
    CommandLineParser parser = new BasicParser();
    try {
      ret = parser.parse(options, args);
    } catch (ParseException e) {
      LOG.warn("Failed to parse input args", e);
    }
    return ret;
  }
}
