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

package alluxio.cli.hms;

import alluxio.cli.AbstractValidationTask;
import alluxio.cli.ValidationTaskResult;
import alluxio.collections.Pair;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * A validation task which includes extra information in order to provide dependencies for
 * sub-tasks along with the task's validation result.
 *
 * @param <K> The input type of this task
 * @param <T> the output type of this task
 */
public abstract class MetastoreValidationTask<K, T> extends AbstractValidationTask {

  @Nullable
  protected final MetastoreValidationTask<?, K> mInputTask;

  /**
   * Create a new {@link MetastoreValidationTask} with the given input task. The input task may
   * be null if the validation is not a subtask.
   *
   * @param inputTask a pre-requisite task which must run before this task
   */
  public MetastoreValidationTask(@Nullable MetastoreValidationTask<?, K> inputTask) {
    mInputTask = inputTask;
  }

  /**
   * @return a result of the validation. If {@link Pair#getFirst()} has a state of
   * {@link alluxio.cli.ValidationUtils.State#OK}, then {@link Pair#getSecond()} will be non-null
   */
  abstract Pair<ValidationTaskResult, T> getValidationWithResult() throws InterruptedException;

  @Override
  public ValidationTaskResult validateImpl(Map<String, String> optionMap)
      throws InterruptedException {
    return getValidationWithResult().getFirst();
  }
}
