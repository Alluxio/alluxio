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

package alluxio.cli.validation;

import org.apache.commons.cli.Option;

import java.util.List;
import java.util.Map;

/**
 * Interface for a validation task run by validateEnv command.
 */
public interface ValidationTask {
  /**
   * @return Set of {@link Option} required by this task
   */
  List<Option> getOptionList();

  /**
   * Checks whether the validation task is applicable.
   *
   * @return true if this validation task should be skipped, false otherwise
   */
  boolean shouldSkip();

  /**
   * Runs the validation task.
   *
   * @param optionMap contains string representation of <key, value> pairs
   * @return whether the validation succeeds
   */
  boolean validate(Map<String, String> optionMap) throws InterruptedException;
}
