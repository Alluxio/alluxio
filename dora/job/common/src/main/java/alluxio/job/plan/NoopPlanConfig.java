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

package alluxio.job.plan;

import com.google.common.base.MoreObjects;

import java.util.Collection;
import java.util.Collections;

/**
 * Config for a plan that does nothing.
 */
public class NoopPlanConfig implements PlanConfig {

  public static final String NAME = "NoOp";
  private static final long serialVersionUID = 1305044963083382523L;

  /**
   * Constructs a new instance.
   */
  public NoopPlanConfig() {}

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    return obj instanceof NoopPlanConfig;
  }

  @Override
  public int hashCode() {
    return 42;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Collection<String> affectedPaths() {
    return Collections.EMPTY_LIST;
  }
}
