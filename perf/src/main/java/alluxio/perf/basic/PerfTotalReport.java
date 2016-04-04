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

/**
 * The abstract class for the total report, which is used to generate a test report by the
 * AlluxioPerfCollector tool. For new test, you should create a new class which extends this.
 */
public abstract class PerfTotalReport {

  protected String mTestCase;

  public void initialSet(String testCase) {
    mTestCase = testCase;
  }

  /**
   * Load the contexts of all the task slaves and initial this total report.
   * 
   * @param taskContexts the contexts for all the task slaves
   * @throws IOException
   */
  public abstract void initialFromTaskContexts(PerfTaskContext[] taskContexts) throws IOException;

  /**
   * Output this total report to file.
   * 
   * @param file the output file
   * @throws IOException
   */
  public abstract void writeToFile(File file) throws IOException;
}
