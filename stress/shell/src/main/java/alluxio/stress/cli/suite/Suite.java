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

package alluxio.stress.cli.suite;

import alluxio.stress.BaseParameters;
import alluxio.stress.Summary;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;

/**
 * Base class for all benchmark suites.
 *
 * @param <T> the summary class
 */
public abstract class Suite<T extends Summary> {
  @ParametersDelegate
  protected BaseParameters mBaseParameters = new BaseParameters();

  /**
   * Runs the suite.
   *
   * @param args the command-line args
   * @return the summary result
   */
  public abstract T runSuite(String[] args) throws Exception;

  void prepare() throws Exception {
    // Implementation can override, if necessary.
  }

  protected static void mainInternal(String[] args, Suite suite) {
    try {
      String result = suite.run(args);
      System.out.println(result);
      System.exit(0);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  private String run(String[] args) throws Exception {
    JCommander jc = new JCommander(this);
    jc.setProgramName(this.getClass().getSimpleName());
    try {
      jc.parse(args);
    } catch (Exception e) {
      jc.usage();
      throw e;
    }

    // prepare the suite.
    prepare();

    // run locally
    T result = runSuite(args);
    return result.toJson();
  }
}
