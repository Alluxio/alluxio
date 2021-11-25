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

package alluxio.stress.cli.suite;

import alluxio.stress.cli.Benchmark;
import alluxio.stress.master.MasterBenchTaskResult;

public class Compact extends Benchmark<MasterBenchTaskResult> {
  @Override
  public String getBenchDescription() {
    return null;
  }

  @Override
  public MasterBenchTaskResult runLocal() throws Exception {
    return null;
  }

  @Override
  public void prepare() throws Exception {

  }
}
