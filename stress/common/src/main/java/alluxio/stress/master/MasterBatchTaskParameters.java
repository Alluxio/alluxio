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

package alluxio.stress.master;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.ImmutableSet;

/**
 * This holds all the parameters for Master Batch Task.
 */
public class MasterBatchTaskParameters extends AbstractMasterBenchParameters {

  @Parameter(description = "The batch task to perform. The current possible option is "
      + "[MasterComprehensiveFileBatchTask]", validateWith = PossibleBatchTaskValidator.class)
  public String mTaskName = "";

  @Parameter(names = {"--cluster"},
      description = "If true, runs the benchmark via the job service cluster. Otherwise, runs "
          + "locally.")
  public boolean mCluster = false;

  @Parameter(names = {"--in-process"},
      description = "If true, runs the task in process. Otherwise, will spawn a new process to "
          + "execute the task",
      hidden = true)
  public boolean mInProcess = false;

  /**
   * a validator use to check the input batch task type.
   */
  public static class PossibleBatchTaskValidator implements IParameterValidator {
    @Override
    public void validate(String name, String value) throws ParameterException {
      ImmutableSet<String> masterBatchTaskNames = ImmutableSet.of(
          "MasterComprehensiveFileBatchTask");
      if (!masterBatchTaskNames.contains(value)) {
        throw new ParameterException(String.format("Unexpected batch task name: %s, "
            + "possible batch task are %s;", value, masterBatchTaskNames));
      }
    }
  }
}
