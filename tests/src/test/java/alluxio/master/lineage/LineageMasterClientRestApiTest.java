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

package alluxio.master.lineage;

import alluxio.AbstractRestApiTest;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.job.Job;
import alluxio.master.AlluxioMaster;
import alluxio.wire.LineageInfo;
import alluxio.wire.LineageInfoTest;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.Map;
import java.util.Random;


@RunWith(PowerMockRunner.class)
@PrepareForTest({LineageMaster.class})
public class LineageMasterClientRestApiTest extends AbstractRestApiTest {
  @Override
  @Test
  public void endpointsTest() throws Exception {
    // Create test input values.
    Map<String, String> createLineageParams = Maps.newHashMap();
    createLineageParams.put("inputFiles", "test");
    createLineageParams.put("outputFiles", "test");
    createLineageParams.put("job.command", "test");
    createLineageParams.put("job.command.conf.outputFile", "test");
    Map<String, String> deleteLineageParams = Maps.newHashMap();
    deleteLineageParams.put("lineageId", "1");
    deleteLineageParams.put("cascade", "false");
    Map<String, String> reinitializeFileParams = Maps.newHashMap();
    reinitializeFileParams.put("path", "test");
    reinitializeFileParams.put("blockSizeBytes", "1");
    reinitializeFileParams.put("ttl", "1");
    Map<String, String> reportLostFileParams = Maps.newHashMap();
    reportLostFileParams.put("path", "test");

    // Generate random return values.
    Random random = new Random();
    long createLineageResult = random.nextLong();
    boolean deleteLineageResult = random.nextBoolean();
    List<LineageInfo> lineageInfos = Lists.newArrayList();
    long numLineageInfos = random.nextInt(10);
    for (int i = 0; i < numLineageInfos; i++) {
      lineageInfos.add(LineageInfoTest.createRandom());
    }
    long reinitializeFileResult = random.nextLong();

    // Set up mocks.
    LineageMaster lineageMaster = PowerMockito.mock(LineageMaster.class);
    Mockito.doReturn(createLineageResult).when(lineageMaster)
        .createLineage(Mockito.<List<AlluxioURI>>any(), Mockito.<List<AlluxioURI>>any(),
            Mockito.<Job>any());
    Mockito.doReturn(deleteLineageResult).when(lineageMaster)
        .deleteLineage(Mockito.anyLong(), Mockito.anyBoolean());
    Mockito.doReturn(lineageInfos).when(lineageMaster).getLineageInfoList();
    Mockito.doReturn(reinitializeFileResult).when(lineageMaster)
        .reinitializeFile(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());
    AlluxioMaster alluxioMaster = PowerMockito.mock(AlluxioMaster.class);
    Mockito.when(alluxioMaster.getLineageMaster()).thenReturn(lineageMaster);
    Whitebox.setInternalState(AlluxioMaster.class, "sAlluxioMaster", alluxioMaster);

    // Create test cases.
    List<TestCase> testCases = Lists.newArrayList();
    testCases.add(new TestCase(LineageMasterClientRestServiceHandler.SERVICE_NAME,
        Maps.<String, String>newHashMap(), "GET", Constants.LINEAGE_MASTER_CLIENT_SERVICE_NAME));
    testCases.add(new TestCase(LineageMasterClientRestServiceHandler.SERVICE_VERSION,
        Maps.<String, String>newHashMap(), "GET", Constants.LINEAGE_MASTER_CLIENT_SERVICE_VERSION));
    testCases.add(
        new TestCase(LineageMasterClientRestServiceHandler.CREATE_LINEAGE, createLineageParams,
            "POST", createLineageResult));
    testCases.add(
        new TestCase(LineageMasterClientRestServiceHandler.DELETE_LINEAGE, deleteLineageParams,
            "POST", deleteLineageResult));
    testCases.add(new TestCase(LineageMasterClientRestServiceHandler.GET_LINEAGE_INFO_LIST,
        Maps.<String, String>newHashMap(), "GET", lineageInfos));
    testCases.add(new TestCase(LineageMasterClientRestServiceHandler.REINITIALIZE_FILE,
        reinitializeFileParams, "POST", reinitializeFileResult));
    testCases.add(
        new TestCase(LineageMasterClientRestServiceHandler.REPORT_LOST_FILE, reportLostFileParams,
            "POST", ""));

    // Execute test cases.
    run(testCases);

    // Verify invocations.
    Mockito.verify(lineageMaster)
        .createLineage(Mockito.<List<AlluxioURI>>any(), Mockito.<List<AlluxioURI>>any(),
            Mockito.<Job>any());
    Mockito.verify(lineageMaster).deleteLineage(Mockito.anyLong(), Mockito.anyBoolean());
    Mockito.verify(lineageMaster).getLineageInfoList();
    Mockito.verify(lineageMaster).reinitializeFile(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(lineageMaster).reportLostFile(Mockito.anyString());
  }
}
