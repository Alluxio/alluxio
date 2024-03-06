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

package alluxio.stress.client;

import alluxio.stress.BaseParameters;
import alluxio.stress.GraphGenerator;
import alluxio.stress.Summary;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The summary for the Hash test.
 */
public class HashTaskSummary implements Summary {
  private static final Logger LOG = LoggerFactory.getLogger(HashTaskSummary.class);
  private List<String> mErrors;
  private BaseParameters mBaseParameters;
  private HashParameters mParameters;

  /**
   * Used for deserialization.
   * */
  @JsonCreator
  public HashTaskSummary() {}

  /**
   * @param result the {@link HashTaskResult} to summarize
   * */
  public HashTaskSummary(HashTaskResult result) {
    mErrors = new ArrayList<>(result.getErrors());
    mBaseParameters = result.getBaseParameters();
    mParameters = result.getParameters();
  }

  /**
   * @return the errors recorded
   * */
  public List<String> getErrors() {
    return mErrors;
  }

  /**
   * @param errors the errors
   * */
  public void setErrors(List<String> errors) {
    mErrors = errors;
  }

  /**
   * @return the {@link BaseParameters}
   * */
  public BaseParameters getBaseParameters() {
    return mBaseParameters;
  }

  /**
   * @param baseParameters the {@link BaseParameters}
   * */
  public void setBaseParameters(BaseParameters baseParameters) {
    mBaseParameters = baseParameters;
  }

  /**
   * @return the task specific {@link HashParameters}
   * */
  public HashParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the {@link HashParameters}
   * */
  public void setParameters(HashParameters parameters) {
    mParameters = parameters;
  }

  @Override
  public String toString() {
    return String.format("HashTaskSummary: {Errors=%s}%n", mErrors);
  }

  @Override
  public GraphGenerator graphGenerator() {
    return null;
  }
}
