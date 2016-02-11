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

package alluxio.master.block;

import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.master.AlluxioMaster;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockInfoTest;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerInfoTest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Random;

import javax.ws.rs.core.Response;

@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockMaster.class)
public class BlockMasterClientRestApiTest {
  @Rule
  public LocalAlluxioClusterResource mResource = new LocalAlluxioClusterResource();

  private URL createURL(String suffix) throws Exception {
    return new URL(
        "http://" + mResource.get().getMasterHostname() + ":" + mResource.get().getMaster()
            .getWebLocalPort() + "/v1/api/" + suffix);
  }

  private String getResponse(HttpURLConnection connection) throws Exception {
    BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
    StringBuffer response = new StringBuffer();

    String inputLine;
    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }
    in.close();

    return response.toString();
  }

  public class TestCase {
    public String mSuffix;
    public String mMethod;
    public Object mExpectedResult;

    public TestCase(String suffix, String method, Object expectedResult) {
      mSuffix = suffix;
      mMethod = method;
      mExpectedResult = expectedResult;
    }

    public String getSuffix() {
      return mSuffix;
    }

    public String getMethod() {
      return mMethod;
    }

    public Object getExpectedResult() {
      return mExpectedResult;
    }
  }

  @Test
  public void endpointsTest() throws Exception {
    BlockInfo blockInfo = BlockInfoTest.createRandom();
    Random random = new Random();
    long capacityBytes = random.nextLong();
    long usedBytes = random.nextLong();
    List<WorkerInfo> workerInfos = Lists.newArrayList();
    workerInfos.add(WorkerInfoTest.createRandom());

    BlockMaster blockMaster = PowerMockito.mock(BlockMaster.class);
    when(blockMaster.getBlockInfo(Mockito.anyLong())).thenReturn(blockInfo);
    when(blockMaster.getCapacityBytes()).thenReturn(capacityBytes);
    when(blockMaster.getUsedBytes()).thenReturn(usedBytes);
    when(blockMaster.getWorkerInfoList()).thenReturn(workerInfos);

    AlluxioMaster alluxioMaster = PowerMockito.mock(AlluxioMaster.class);
    when(alluxioMaster.getBlockMaster()).thenReturn(blockMaster);
    Whitebox.setInternalState(AlluxioMaster.class, "sAlluxioMaster", alluxioMaster);

    List<TestCase> testCases = Lists.newArrayList();
    testCases.add(new TestCase(BlockMasterClientRestServiceHandler.SERVICE_NAME, "GET",
        Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME));
    testCases.add(new TestCase(BlockMasterClientRestServiceHandler.SERVICE_VERSION, "GET",
        Constants.BLOCK_MASTER_CLIENT_SERVICE_VERSION));
    testCases
        .add(new TestCase(BlockMasterClientRestServiceHandler.GET_BLOCK_INFO, "GET", blockInfo));
    testCases.add(
        new TestCase(BlockMasterClientRestServiceHandler.GET_CAPACITY_BYTES, "GET", capacityBytes));
    testCases
        .add(new TestCase(BlockMasterClientRestServiceHandler.GET_USED_BYTES, "GET", usedBytes));
    testCases.add(
        new TestCase(BlockMasterClientRestServiceHandler.GET_WORKER_INFO_LIST, "GET", workerInfos));

    for (TestCase testCase : testCases) {
      HttpURLConnection connection =
          (HttpURLConnection) createURL(testCase.getSuffix()).openConnection();
      connection.setRequestMethod(testCase.getMethod());
      connection.connect();
      Assert.assertEquals(connection.getResponseCode(), Response.Status.OK.getStatusCode());
      ObjectMapper mapper = new ObjectMapper();
      String expected = mapper.writeValueAsString(testCase.getExpectedResult());
      expected = expected.replaceAll("^\"|\"$", ""); // needed to handle string return values
      Assert.assertEquals(expected, getResponse(connection));
    }
  }
}
