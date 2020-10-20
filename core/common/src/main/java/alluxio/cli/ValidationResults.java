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

import java.util.List;
import java.util.Map;

/**
 * A container class for the validation results.
 */
public class ValidationResults {
  private Map<ValidationUtils.State, List<ValidationTaskResult>> mResult;

  /**
   * Set validation results.
   *
   * @param result validation result
   */
  public void setResult(Map<ValidationUtils.State, List<ValidationTaskResult>> result) {
    mResult = result;
  }

  /**
   * Get Validation Result.
   *
   * @return validation result
   */
  public Map<ValidationUtils.State, List<ValidationTaskResult>> getResult() {
    return mResult;
  }
}
