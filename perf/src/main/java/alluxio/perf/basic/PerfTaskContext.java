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

import org.apache.log4j.Logger;

import alluxio.perf.PerfConstants;

/**
 * The abstract class for the context, which contains the statistics of the test. For new test, you
 * should create a new class which extends this.
 */
public abstract class PerfTaskContext {
  protected static final Logger LOG = Logger.getLogger(PerfConstants.PERF_LOGGER_TYPE);

  protected int mId;
  protected String mNodeName;
  protected String mTestCase;

  // The basic statistics of a test.
  protected long mFinishTimeMs;
  protected long mStartTimeMs;
  protected boolean mSuccess;

  public void initialSet(int id, String nodeName, String testCase, TaskConfiguration taskConf) {
    mId = id;
    mNodeName = nodeName;
    mTestCase = testCase;
    mStartTimeMs = System.currentTimeMillis();
    mFinishTimeMs = mStartTimeMs;
    mSuccess = true;
  }

  public long getFinishTimeMs() {
    return mFinishTimeMs;
  }

  public int getId() {
    return mId;
  }

  public String getNodeName() {
    return mNodeName;
  }

  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  public boolean getSuccess() {
    return mSuccess;
  }

  public String getTestCase() {
    return mTestCase;
  }

  /**
   * Load this task context from file.
   * 
   * @param file the input file
   * @throws IOException
   */
  public abstract void loadFromFile(File file) throws IOException;

  public void setFinishTimeMs(long finishTimeMs) {
    mFinishTimeMs = finishTimeMs;
  }

  /**
   * Set those statistics from the threads' results.
   * 
   * @param threads
   */
  public abstract void setFromThread(PerfThread[] threads);

  public void setStartTimeMs(long startTimeMs) {
    mStartTimeMs = startTimeMs;
  }

  public void setSuccess(boolean success) {
    mSuccess = success;
  }

  /**
   * Output this task context to file.
   * 
   * @param file the output file
   * @throws IOException
   */
  public abstract void writeToFile(File file) throws IOException;
}
