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

package alluxio.stress.cli;

import alluxio.stress.TaskResult;
import alluxio.stress.common.FileSystemParameters;

import com.beust.jcommander.ParametersDelegate;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * An abstraction for stress tests which generates a specific workload
 * on a certain Alluxio component (master, worker, ClientIO).
 *
 * @param <T> the type of task result
 * @param <P> the type of task parameter
 */
public abstract class AbstractStressBench<T extends TaskResult, P extends FileSystemParameters>
    extends Benchmark<T> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractStressBench.class);
  @ParametersDelegate
  protected P mParameters;

  /**
   * Validates the parameters.
   */
  public void validateParams() throws Exception {}

  @Override
  public String run(String[] args) throws Exception {
    parseParameters(args);
    validateParams();
    // if the benchmark execute multiple tasks
    if (mParameters.mWriteType.equals("ALL")) {
      List<String> writeTypes = ImmutableList.of("MUST_CACHE", "CACHE_THROUGH",
          "ASYNC_THROUGH", "THROUGH");
      System.out.format("Now executing %s with all possible write "
              + "types %s %n", getClass().toString(), writeTypes);

      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("--write-type")) {
          for (String type : writeTypes) {
            // change write type argument
            args[i + 1] = type;
            mParameters.mWriteType = type;

            System.out.println("-----------------------------------------------------");
            System.out.format("Now executing write type %s... %n", type);

            try {
              String result = runSingleTask(args);
              System.out.println(result);
            } catch (Exception e) {
              System.out.format("Exception occurred when executing parameter"
                  + " --write-type %s %n", type);
              System.out.println(e.getMessage());
            }
          }
          System.out.println("-----------------------------------------------------");
          return "All tasks finished. You can find the test results in the outputs above.";
        }
      }
    }

    // if this is a single task, just execute the task
    return runSingleTask(args);
  }
}
