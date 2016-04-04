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

import org.apache.log4j.Logger;

import alluxio.perf.PerfConstants;

/**
 * The abstract class for the thread, which contains the executing logic of the test. For new test,
 * you should create a new class which extends this.
 */
public abstract class PerfThread implements Runnable {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  protected int mId;
  protected String mNodeName;
  protected int mTaskId;
  protected String mTestCase;

  public void initialSet(int threadId, int taskId, String nodeName, String testCase) {
    mId = threadId;
    mTaskId = taskId;
    mNodeName = nodeName;
    mTestCase = testCase;
  }

  /**
   * Cleanup the thread. Do some following work.
   * 
   * @param taskConf
   * @return true if cleanup successfully, false otherwise
   */
  public abstract boolean cleanupThread(TaskConfiguration taskConf);

  /**
   * Setup the thread. Do some preparations.
   * 
   * @param taskConf
   * @return true if setup successfully, false otherwise
   */
  public abstract boolean setupThread(TaskConfiguration taskConf);
}
