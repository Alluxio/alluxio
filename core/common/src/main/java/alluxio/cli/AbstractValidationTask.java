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

package alluxio.cli;

import org.apache.commons.cli.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Abstract class for validation environment.
 */
public abstract class AbstractValidationTask implements ValidationTask {
  @Override
  public List<Option> getOptionList() {
    return new ArrayList<>();
  }

  protected abstract ValidationTaskResult validateImpl(Map<String, String> optionMap)
      throws InterruptedException;

  /**
   * validate a test and return the result.
   *
   * @param optionMap contains string representation of <key, value> pairs
   * @return return test result
   */
  public ValidationTaskResult validate(Map<String, String> optionMap)
      throws InterruptedException {
    try {
      return validateImpl(optionMap);
    } catch (InterruptedException e) {
      throw e;
    } catch (Exception e) {
      return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
          ValidationUtils.getErrorInfo(e), "Fix unexpected error");
    }
  }
}
