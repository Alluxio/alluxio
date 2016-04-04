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

package alluxio.perf.basic;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import alluxio.perf.PerfConstants;
import alluxio.perf.conf.PerfConf;

/**
 * The abstract class for all the test tasks. For new test, you should create a new class which
 * extends this.
 */
public abstract class PerfTask {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  protected int mId;
  protected int mTotalTasks;
  protected String mNodeName;
  protected TaskConfiguration mTaskConf;
  protected String mTestCase;

  private PerfThread[] mThreads;

  public void initialSet(int id, int totalTasks, String nodeName, TaskConfiguration taskConf,
      String testCase) {
    mId = id;
    mTotalTasks = totalTasks;
    mNodeName = nodeName;
    mTaskConf = taskConf;
    mTestCase = testCase;
  }


  public boolean cleanup(PerfTaskContext taskContext) {
    boolean ret = true;
    for (int i = 0; i < mThreads.length; i ++) {
      ret &= mThreads[i].cleanupThread(mTaskConf);
    }
    ret &= cleanupTask(taskContext);
    taskContext.setFromThread(mThreads);
    taskContext.setFinishTimeMs(System.currentTimeMillis());
    try {
      String outDirPath = PerfConf.get().OUT_FOLDER;
      File outDir = new File(outDirPath);
      if (!outDir.exists()) {
        outDir.mkdirs();
      }
      String reportFileName =
          outDirPath + "/" + PerfConstants.PERF_CONTEXT_FILE_NAME_PREFIX + mTestCase + "-" + mId
              + "@" + mNodeName;
      taskContext.writeToFile(new File(reportFileName));
    } catch (IOException e) {
      LOG.error("Error when generate the task report", e);
      ret = false;
    }
    return ret;
  }

  /**
   * Cleanup the task. Do some following work.
   *
   * @param taskContext the context of this task
   * @return true if cleanup successfully, false otherwise
   */
  protected abstract boolean cleanupTask(PerfTaskContext taskContext);

  /**
   * If you want to cleanup certain work directory after the whole test finished, return in here.
   * Otherwise return null.
   *
   * @return the work directory to cleanup, otherwise null
   */
  public abstract String getCleanupDir();

  public boolean run(PerfTaskContext taskContext) {
    List<Thread> threadList = new ArrayList<Thread>(mThreads.length);
    try {
      for (int i = 0; i < mThreads.length; i ++) {
        Thread t = new Thread(mThreads[i]);
        threadList.add(t);
      }
      for (Thread t : threadList) {
        t.start();
      }
      for (Thread t : threadList) {
        t.join();
      }
    } catch (InterruptedException e) {
      LOG.error("Error when wait all threads", e);
      return false;
    } catch (Exception e) {
      LOG.error("Error to create task thread", e);
      return false;
    }
    return true;
  }

  public boolean setup(PerfTaskContext taskContext) {
    taskContext.setStartTimeMs(System.currentTimeMillis());
    boolean ret = setupTask(taskContext);

    int threadNum = mTaskConf.getIntProperty("threads.num");
    threadNum = threadNum==0?PerfConf.get().THREADS_NUM:threadNum;

    mThreads = new PerfThread[threadNum];
    try {
      for (int i = 0; i < mThreads.length; i ++) {
        mThreads[i] = TestCase.get().getTaskThreadClass(mTestCase);
        mThreads[i].initialSet(i, mId, mNodeName, mTestCase);
        ret &= mThreads[i].setupThread(mTaskConf);
      }
    } catch (Exception e) {
      LOG.error("Error to create task thread", e);
      return false;
    }
    return ret;
  }

  /**
   * Setup the task. Do some preparations.
   *
   * @param taskContext the context of this task
   * @return true if setup successfully, false otherwise
   */
  protected abstract boolean setupTask(PerfTaskContext taskContext);
}
