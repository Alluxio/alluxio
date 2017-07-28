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
import alluxio.LocalAlluxioClusterResource.Config;
import alluxio.PropertyKey;
import alluxio.client.file.FileSystemContext;
import alluxio.client.lineage.LineageContext;
import alluxio.client.lineage.LineageFileSystem;
import alluxio.job.CommandLineJob;
import alluxio.job.JobConf;
import alluxio.master.MasterProcess;
import alluxio.rest.RestApiTest;
import alluxio.rest.TestCase;
import alluxio.wire.LineageInfo;
import alluxio.wire.TtlAction;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.HttpMethod;

/**
 * Test cases for {@link LineageMasterClientRestServiceHandler}.
 */
public final class LineageMasterClientRestApiTest extends RestApiTest {
  private MasterProcess mMasterProcess;
  private LineageFileSystem mLineageClient;

  @Before
  public void before() throws Exception {
    mHostname = mResource.get().getHostname();
    mPort = mResource.get().getLocalAlluxioMaster().getMasterProcess().getWebAddress().getPort();
    mServicePrefix = LineageMasterClientRestServiceHandler.SERVICE_PREFIX;
    mLineageClient = LineageFileSystem.get(FileSystemContext.INSTANCE, LineageContext.INSTANCE);
    mMasterProcess = mResource.get().getLocalAlluxioMaster().getMasterProcess();
  }

  @Test
  @Config(confParams = {PropertyKey.Name.USER_LINEAGE_ENABLED, "true"})
  public void serviceName() throws Exception {
    new TestCase(mHostname, mPort, getEndpoint(LineageMasterClientRestServiceHandler.SERVICE_NAME),
        NO_PARAMS, HttpMethod.GET, Constants.LINEAGE_MASTER_CLIENT_SERVICE_NAME).run();
  }

  @Test
  @Config(confParams = {PropertyKey.Name.USER_LINEAGE_ENABLED, "true"})
  public void serviceVersion() throws Exception {
    new TestCase(mHostname, mPort,
        getEndpoint(LineageMasterClientRestServiceHandler.SERVICE_VERSION), NO_PARAMS,
        HttpMethod.GET, Constants.LINEAGE_MASTER_CLIENT_SERVICE_VERSION).run();
  }

  @Test
  @Config(confParams = {PropertyKey.Name.USER_LINEAGE_ENABLED, "true"})
  public void createLineage() throws Exception {
    mLineageClient.createFile(new AlluxioURI("/input")).close();

    Map<String, String> params = new HashMap<>();
    params.put("inputFiles", "/input");
    params.put("outputFiles", "/output");
    params.put("command", "test");
    params.put("commandOutputFile", "test");
    new TestCase(mHostname, mPort,
        getEndpoint(LineageMasterClientRestServiceHandler.CREATE_LINEAGE), params, HttpMethod.POST,
        null).call();
  }

  @Test
  @Config(confParams = {PropertyKey.Name.USER_LINEAGE_ENABLED, "true"})
  public void deleteLineage() throws Exception {
    LineageMaster lineageMaster = mMasterProcess.getMaster(LineageMaster.class);
    long lineageId = lineageMaster.createLineage(new ArrayList<AlluxioURI>(),
        new ArrayList<AlluxioURI>(), new CommandLineJob("test", new JobConf("/output")));

    Map<String, String> params = new HashMap<>();
    params.put("lineageId", Long.toString(lineageId));
    params.put("cascade", "false");
    new TestCase(mHostname, mPort,
        getEndpoint(LineageMasterClientRestServiceHandler.DELETE_LINEAGE), params, HttpMethod.POST,
        true).run();
  }

  @Test
  @Config(confParams = {PropertyKey.Name.USER_LINEAGE_ENABLED, "true"})
  public void getLineageInfoList() throws Exception {
    AlluxioURI input = new AlluxioURI("/input");
    AlluxioURI output = new AlluxioURI("/output");
    LineageMaster lineageMaster = mMasterProcess.getMaster(LineageMaster.class);
    mLineageClient.createFile(new AlluxioURI("/input")).close();
    long lineageId = lineageMaster.createLineage(Lists.newArrayList(input),
        Lists.newArrayList(output), new CommandLineJob("test", new JobConf(output.getPath())));

    String result = new TestCase(mHostname, mPort,
        getEndpoint(LineageMasterClientRestServiceHandler.GET_LINEAGE_INFO_LIST), NO_PARAMS,
        HttpMethod.GET, null).call();
    List<LineageInfo> lineageInfos =
        new ObjectMapper().readValue(result, new TypeReference<List<LineageInfo>>() {});
    LineageInfo lineageInfo = Iterables.getOnlyElement(lineageInfos);
    Assert.assertEquals(ImmutableList.of(input.toString()), lineageInfo.getInputFiles());
    Assert.assertEquals(ImmutableList.of(output.toString()), lineageInfo.getOutputFiles());
    Assert.assertEquals(lineageId, lineageInfo.getId());
  }

  @Test
  @Config(confParams = {PropertyKey.Name.USER_LINEAGE_ENABLED, "true"})
  public void reinitializeFile() throws Exception {
    mLineageClient.createFile(new AlluxioURI("/test")).close();

    Map<String, String> params = new HashMap<>();
    params.put("path", "/test");
    params.put("blockSizeBytes", "1");
    params.put("ttl", "1");
    params.put("ttlAction", TtlAction.DELETE.toString());

    new TestCase(mHostname, mPort,
        getEndpoint(LineageMasterClientRestServiceHandler.REINITIALIZE_FILE), params,
        HttpMethod.POST, null).call();
  }

  @Test
  @Config(confParams = {PropertyKey.Name.USER_LINEAGE_ENABLED, "true"})
  public void reportLostFile() throws Exception {
    mLineageClient.createFile(new AlluxioURI("/test")).close();

    Map<String, String> params = new HashMap<>();
    params.put("path", "/test");

    new TestCase(mHostname, mPort,
        getEndpoint(LineageMasterClientRestServiceHandler.REPORT_LOST_FILE), params,
        HttpMethod.POST, null).run();
  }
}
