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

package alluxio.stress.common;

import alluxio.stress.Parameters;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.ImmutableSet;

/**
 * Store the operation parameters used in batch task.
 */
public class BatchTaskParameters extends Parameters {
  @Parameter(description = "The batch task to perform. The current possible option is "
      + "[MasterComprehensiveFileBatchTask]", validateWith = PossibleBatchTaskValidator.class)
  public String mTaskName = "";

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
