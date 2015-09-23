/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.job;

import java.io.Serializable;

import com.google.common.base.Preconditions;

/**
 * A Job that can run at Tachyon.
 */
@SuppressWarnings("serial")
public abstract class Job implements Serializable {
  private final JobConf mJobConf;

  public Job(JobConf jobConf) {
    mJobConf = Preconditions.checkNotNull(jobConf);
  }

  public JobConf getJobConf() {
    return mJobConf;
  }

  public abstract boolean run();
}
