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

package alluxio.master.lineage;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.job.Job;
import alluxio.master.AlluxioMaster;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.wire.LineageInfo;
import alluxio.wire.LineageInfoTest;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link LineageMasterClientRestServiceHandler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LineageMaster.class})
public final class LineageMasterClientRestApiTest extends RestApiTest {
  private LineageMaster mLineageMaster;

  @Before
  public void before() throws Exception {
    AlluxioMaster alluxioMaster = mResource.get().getMaster().getInternalMaster();
    mLineageMaster = PowerMockito.mock(LineageMaster.class);
    // Replace any lineage master created by LocalAlluxioClusterResource with a mock.
    LineageMaster lineageMaster = Whitebox.getInternalState(alluxioMaster, "mLineageMaster");
    if (lineageMaster != null) {
      lineageMaster.stop();
    }
    Whitebox.setInternalState(alluxioMaster, "mLineageMaster", mLineageMaster);
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getMaster().getWebLocalPort();
    mServicePrefix = LineageMasterClientRestServiceHandler.SERVICE_PREFIX;
  }

  @Test
  public void serviceNameTest() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(LineageMasterClientRestServiceHandler.SERVICE_NAME),
        NO_PARAMS, HttpMethod.GET, Constants.LINEAGE_MASTER_CLIENT_SERVICE_NAME).run();
  }

  @Test
  public void serviceVersionTest() throws Exception {
    new TestCase(mHostname, mPort,
        getEndpoint(LineageMasterClientRestServiceHandler.SERVICE_VERSION), NO_PARAMS,
        HttpMethod.GET, Constants.LINEAGE_MASTER_CLIENT_SERVICE_VERSION).run();
  }

  @Test
  public void createLineageTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("inputFiles", "test");
    params.put("outputFiles", "test");
    params.put("command", "test");
    params.put("commandOutputFile", "test");

    Random random = new Random();
    long result = random.nextLong();
    Mockito.doReturn(result).when(mLineageMaster)
        .createLineage(Mockito.<List<AlluxioURI>>any(), Mockito.<List<AlluxioURI>>any(),
            Mockito.<Job>any());

    new TestCase(mHostname, mPort,
        getEndpoint(LineageMasterClientRestServiceHandler.CREATE_LINEAGE), params, HttpMethod.POST,
        result).run();

    Mockito.verify(mLineageMaster)
        .createLineage(Mockito.<List<AlluxioURI>>any(), Mockito.<List<AlluxioURI>>any(),
            Mockito.<Job>any());
  }

  @Test
  public void deleteLineageTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("lineageId", "1");
    params.put("cascade", "false");

    Random random = new Random();
    boolean result = random.nextBoolean();
    Mockito.doReturn(result).when(mLineageMaster)
        .deleteLineage(Mockito.anyLong(), Mockito.anyBoolean());

    new TestCase(mHostname, mPort,
        getEndpoint(LineageMasterClientRestServiceHandler.DELETE_LINEAGE), params, HttpMethod.POST,
        result).run();

    Mockito.verify(mLineageMaster).deleteLineage(Mockito.anyLong(), Mockito.anyBoolean());
  }

  @Test
  public void getLineageInfoListTest() throws Exception {
    Random random = new Random();
    List<LineageInfo> lineageInfos = new ArrayList<>();
    long numLineageInfos = random.nextInt(10);
    for (int i = 0; i < numLineageInfos; i++) {
      lineageInfos.add(LineageInfoTest.createRandom());
    }
    Mockito.doReturn(lineageInfos).when(mLineageMaster).getLineageInfoList();

    new TestCase(mHostname, mPort,
        getEndpoint(LineageMasterClientRestServiceHandler.GET_LINEAGE_INFO_LIST), NO_PARAMS,
        HttpMethod.GET, lineageInfos).run();

    Mockito.verify(mLineageMaster).getLineageInfoList();
  }

  @Test
  public void reinitializeFileTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("path", "test");
    params.put("blockSizeBytes", "1");
    params.put("ttl", "1");

    Random random = new Random();
    long result = random.nextLong();
    Mockito.doReturn(result).when(mLineageMaster)
        .reinitializeFile(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());

    new TestCase(mHostname, mPort,
        getEndpoint(LineageMasterClientRestServiceHandler.REINITIALIZE_FILE), params,
        HttpMethod.POST, result).run();

    Mockito.verify(mLineageMaster)
        .reinitializeFile(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());
  }

  @Test
  public void reportLostFileTest() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("path", "test");

    new TestCase(mHostname, mPort,
        getEndpoint(LineageMasterClientRestServiceHandler.REPORT_LOST_FILE), params,
        HttpMethod.POST, null).run();

    Mockito.verify(mLineageMaster).reportLostFile(Mockito.anyString());
  }
}
