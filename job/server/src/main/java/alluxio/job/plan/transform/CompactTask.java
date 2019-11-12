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

package alluxio.job.plan.transform;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * A task for a job worker to compact files into one file.
 */
public final class CompactTask implements Serializable {
  private static final long serialVersionUID = -998740998086570018L;

  private final ArrayList<String> mInputs;
  private final String mOutput;

  /**
   * @param inputs the input files to be compacted
   * @param output the compacted file
   */
  public CompactTask(ArrayList<String> inputs, String output) {
    mInputs = inputs;
    mOutput = output;
  }

  /**
   * @return the inputs
   */
  public ArrayList<String> getInputs() {
    return mInputs;
  }

  /**
   * @return the output
   */
  public String getOutput() {
    return mOutput;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CompactTask)) {
      return false;
    }
    CompactTask that = (CompactTask) o;
    return mInputs.equals(that.mInputs)
        && mOutput.equals(that.mOutput);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mInputs, mOutput);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("inputs", mInputs)
        .add("output", mOutput)
        .toString();
  }
}
