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

package alluxio.job;

import alluxio.job.plan.PlanConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;

/**
 * The configuration of a dummy job for testing.
 */
@ThreadSafe
@JsonTypeName(TestPlanConfig.NAME)
public class TestPlanConfig implements PlanConfig {
  public static final String NAME = "Test";

  private static final long serialVersionUID = -7937106659935180792L;
  private final String mFilePath;

  /**
   * @param filePath the file path
   */
  public TestPlanConfig(@JsonProperty("filePath") String filePath) {
    mFilePath = Preconditions.checkNotNull(filePath, "The file path cannot be null");
  }

  /**
   * @return the file path
   */
  public String getFilePath() {
    return mFilePath;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TestPlanConfig)) {
      return false;
    }
    TestPlanConfig that = (TestPlanConfig) obj;
    return mFilePath.equals(that.mFilePath);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mFilePath);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("FilePath", mFilePath).toString();
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Collection<String> affectedPaths() {
    return ImmutableList.of(mFilePath);
  }
}
