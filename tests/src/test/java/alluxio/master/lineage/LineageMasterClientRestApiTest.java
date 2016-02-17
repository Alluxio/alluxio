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

import alluxio.LocalAlluxioClusterResource;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.job.Job;
import alluxio.master.AlluxioMaster;
import alluxio.rest.TestCaseFactory;
import alluxio.wire.LineageInfo;
import alluxio.wire.LineageInfoTest;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.BeforeClass;
import org.junit.Rule;
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

/**
 * Test cases for {@link LineageMasterClientRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LineageMaster.class})
public class LineageMasterClientRestApiTest {
  private static final Map<String, String> NO_PARAMS = Maps.newHashMap();
  private static LineageMaster sLineageMaster;

  @Rule
  private LocalAlluxioClusterResource mResource = new LocalAlluxioClusterResource();

  @BeforeClass
  public static void beforeClass() {
    sLineageMaster = PowerMockito.mock(LineageMaster.class);
    AlluxioMaster alluxioMaster = PowerMockito.mock(AlluxioMaster.class);
    Mockito.doReturn(sLineageMaster).when(alluxioMaster).getLineageMaster();
    Whitebox.setInternalState(AlluxioMaster.class, "sAlluxioMaster", alluxioMaster);
  }

  private String getEndpoint(String suffix) {
    return LineageMasterClientRestServiceHandler.SERVICE_PREFIX + "/" + suffix;
  }

  @Test
  public void serviceNameTest() throws Exception {
    TestCaseFactory
        .newMasterTestCase(getEndpoint(LineageMasterClientRestServiceHandler.SERVICE_NAME),
            NO_PARAMS, "GET", Constants.LINEAGE_MASTER_CLIENT_SERVICE_NAME, mResource).run();
  }

  @Test
  public void serviceVersionTest() throws Exception {
    TestCaseFactory
        .newMasterTestCase(getEndpoint(LineageMasterClientRestServiceHandler.SERVICE_VERSION),
            NO_PARAMS, "GET", Constants.LINEAGE_MASTER_CLIENT_SERVICE_VERSION, mResource).run();
  }

  @Test
  public void createLineageTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("inputFiles", "test");
    params.put("outputFiles", "test");
    params.put("command", "test");
    params.put("commandOutputFile", "test");

    Random random = new Random();
    long result = random.nextLong();
    Mockito.doReturn(result).when(sLineageMaster)
        .createLineage(Mockito.<List<AlluxioURI>>any(), Mockito.<List<AlluxioURI>>any(),
            Mockito.<Job>any());

    TestCaseFactory
        .newMasterTestCase(getEndpoint(LineageMasterClientRestServiceHandler.CREATE_LINEAGE),
            params, "POST", result, mResource).run();

    Mockito.verify(sLineageMaster)
        .createLineage(Mockito.<List<AlluxioURI>>any(), Mockito.<List<AlluxioURI>>any(),
            Mockito.<Job>any());
  }

  @Test
  public void deleteLineageTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("lineageId", "1");
    params.put("cascade", "false");

    Random random = new Random();
    boolean result = random.nextBoolean();
    Mockito.doReturn(result).when(sLineageMaster)
        .deleteLineage(Mockito.anyLong(), Mockito.anyBoolean());

    TestCaseFactory
        .newMasterTestCase(getEndpoint(LineageMasterClientRestServiceHandler.DELETE_LINEAGE),
            params, "POST", result, mResource).run();

    Mockito.verify(sLineageMaster).deleteLineage(Mockito.anyLong(), Mockito.anyBoolean());
  }

  @Test
  public void getLineageInfoListTest() throws Exception {
    Random random = new Random();
    List<LineageInfo> lineageInfos = Lists.newArrayList();
    long numLineageInfos = random.nextInt(10);
    for (int i = 0; i < numLineageInfos; i++) {
      lineageInfos.add(LineageInfoTest.createRandom());
    }
    Mockito.doReturn(lineageInfos).when(sLineageMaster).getLineageInfoList();

    TestCaseFactory
        .newMasterTestCase(getEndpoint(LineageMasterClientRestServiceHandler.GET_LINEAGE_INFO_LIST),
            NO_PARAMS, "GET", lineageInfos, mResource).run();

    Mockito.verify(sLineageMaster).getLineageInfoList();
  }

  @Test
  public void reinitializeFileTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");
    params.put("blockSizeBytes", "1");
    params.put("ttl", "1");

    Random random = new Random();
    long result = random.nextLong();
    Mockito.doReturn(result).when(sLineageMaster)
        .reinitializeFile(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());

    TestCaseFactory
        .newMasterTestCase(getEndpoint(LineageMasterClientRestServiceHandler.REINITIALIZE_FILE),
            params, "POST", result, mResource).run();

    Mockito.verify(sLineageMaster)
        .reinitializeFile(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void reportLostFileTest() throws Exception {
    Map<String, String> params = Maps.newHashMap();
    params.put("path", "test");

    TestCaseFactory
        .newMasterTestCase(getEndpoint(LineageMasterClientRestServiceHandler.REPORT_LOST_FILE),
            params, "POST", "", mResource).run();

    Mockito.verify(sLineageMaster).reportLostFile(Mockito.anyString());
  }
}
