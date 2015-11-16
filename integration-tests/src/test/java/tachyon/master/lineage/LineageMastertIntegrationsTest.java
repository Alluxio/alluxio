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

package tachyon.master.lineage;

import java.util.List;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.client.lineage.LineageMasterClient;
import tachyon.job.CommandLineJob;
import tachyon.job.JobConf;
import tachyon.master.lineage.meta.LineageFileState;
import tachyon.thrift.LineageInfo;

public final class LineageMastertIntegrationsTest {
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(1000, 1000, Constants.GB);

  private static final String OUT_FILE = "/test";

  @Test
  @LocalTachyonClusterResource.Config(tachyonConfParams = {Constants.USER_LINEAGE_ENABLED, "true"})
  public void lineageCreationTest() throws Exception {
    LineageMasterClient lineageMasterClient =
        new LineageMasterClient(mLocalTachyonClusterResource.get().getMaster().getAddress(),
            mLocalTachyonClusterResource.get().getMasterTachyonConf());
    try {
      CommandLineJob job = new CommandLineJob("test", new JobConf("output"));
      lineageMasterClient.createLineage(Lists.<String>newArrayList(), Lists.newArrayList(OUT_FILE),
          job);

      List<LineageInfo> infos = lineageMasterClient.getLineageInfoList();
      Assert.assertEquals(1, infos.size());
      Assert.assertEquals(LineageFileState.CREATED.toString(),
          infos.get(0).outputFiles.get(0).state);
    } finally {
      lineageMasterClient.close();
    }
  }

}
