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

package alluxio.hub.agent.rpc.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import alluxio.conf.PropertyKey;
import alluxio.hub.agent.process.AgentProcessContext;
import alluxio.hub.agent.process.TestAgentProcessContextFactory;
import alluxio.hub.agent.util.process.ProcessLauncher;
import alluxio.hub.proto.AgentGetConfigurationSetRequest;
import alluxio.hub.proto.AgentListCatalogRequest;
import alluxio.hub.proto.AgentListCatalogResponse;
import alluxio.hub.proto.AgentManagerServiceGrpc;
import alluxio.hub.proto.AgentProcessStatusChangeRequest;
import alluxio.hub.proto.AgentValidatePrestoConfRequest;
import alluxio.hub.proto.AgentWriteConfigurationSetRequest;
import alluxio.hub.proto.AlluxioConfigurationSet;
import alluxio.hub.proto.AlluxioNodeType;
import alluxio.hub.proto.PrestoCatalogListing;
import alluxio.hub.proto.ProcessStateChange;
import alluxio.hub.proto.ValidationStatus;
import alluxio.hub.test.BaseHubTest;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class AgentManagerServiceTest extends BaseHubTest {

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  private AgentProcessContext mContext;
  private String mServerName;
  private Server mServer;
  private ManagedChannel mChannel;
  private AgentManagerServiceGrpc.AgentManagerServiceBlockingStub mClient;
  private ProcessLauncher mLauncher;

  @Before
  public void before() throws Exception {
    mLauncher = mock(ProcessLauncher.class);
    mContext = Mockito.spy(TestAgentProcessContextFactory.newContext(
        Collections.singleton(AlluxioNodeType.MASTER),
        mLauncher, getTestConfig()));
    Properties prop = new Properties();
    Path catalog = Paths.get(TEST_PRESTO_CONF_DIR.getRoot().getCanonicalPath(),
        "catalog");
    if (Files.exists(catalog)) {
      Files.walk(catalog).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (IOException e) {
          return;
        }
      });
      Files.deleteIfExists(catalog);
    }
    Files.createDirectory(catalog);
    prop.put(PropertyKey.HUB_MANAGER_PRESTO_CONF_PATH.getName(),
        TEST_PRESTO_CONF_DIR.getRoot().getCanonicalPath());
    mContext.updateConf(prop);
    doNothing().when(mContext).restartPresto();
    mServerName = InProcessServerBuilder.generateName();
    mServer = InProcessServerBuilder.forName(mServerName)
        .directExecutor()
        .addService(new AgentManagerService(mContext))
        .build();
    mServer.start();
    mChannel = InProcessChannelBuilder.forName(mServerName).directExecutor().build();
    mClient = AgentManagerServiceGrpc.newBlockingStub(mChannel);
  }

  @After
  public void after() throws Exception {
    mChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    mServer.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  public void testAgentStopAction() throws Exception {
    AgentProcessStatusChangeRequest r = AgentProcessStatusChangeRequest.newBuilder()
        .setNodeType(AlluxioNodeType.MASTER)
        .setAction(ProcessStateChange.STOP)
        .build();
    mClient.processStatusChange(r);
    verify(mLauncher).stop(AlluxioNodeType.MASTER);
  }

  @Test
  public void testAgentStartAction() throws Exception {
    AgentProcessStatusChangeRequest r = AgentProcessStatusChangeRequest.newBuilder()
        .setNodeType(AlluxioNodeType.MASTER)
        .setAction(ProcessStateChange.START)
        .build();
    mClient.processStatusChange(r);
    verify(mLauncher).start(AlluxioNodeType.MASTER);
  }

  @Test
  public void testAgentRestartAction() throws Exception {
    AgentProcessStatusChangeRequest r = AgentProcessStatusChangeRequest.newBuilder()
        .setNodeType(AlluxioNodeType.MASTER)
        .setAction(ProcessStateChange.RESTART)
        .build();
    mClient.processStatusChange(r);
    verify(mLauncher).stop(AlluxioNodeType.MASTER);
    verify(mLauncher).start(AlluxioNodeType.MASTER);
  }

  @Test
  public void testStateChangeThrowException() throws Exception {
    doThrow(new IOException("spied")).when(mContext).changeState(ArgumentMatchers.any(),
        ArgumentMatchers.any());
    AgentProcessStatusChangeRequest r = AgentProcessStatusChangeRequest.newBuilder()
        .setNodeType(AlluxioNodeType.MASTER)
        .setAction(ProcessStateChange.RESTART)
        .build();
    assertThrows(StatusRuntimeException.class, () -> {
      mClient.processStatusChange(r);
    });
  }

  @Test
  public void testGetConf() throws Exception {
    AlluxioConfigurationSet s = AlluxioConfigurationSet.newBuilder()
        .setSiteProperties("testProps")
        .setAlluxioEnv("testEnv")
        .build();
    doReturn(s).when(mContext).getConf();
    AgentGetConfigurationSetRequest r = AgentGetConfigurationSetRequest.newBuilder().build();
    assertEquals(s, mClient.getConfigurationSet(r).getConfSet());
  }

  @Test
  public void testWriteConf() throws Exception {
    AlluxioConfigurationSet s = AlluxioConfigurationSet.newBuilder()
        .setSiteProperties("testProps")
        .setAlluxioEnv("testEnv")
        .build();
    doNothing().when(mContext).writeConf(ArgumentMatchers.any());
    AgentWriteConfigurationSetRequest r = AgentWriteConfigurationSetRequest.newBuilder()
        .setConfSet(s)
        .build();
    mClient.writeConfigurationSet(r);
    verify(mContext).writeConf(s);
  }

  @Test
  public void testListCatalogs() throws Exception {
    String confDir = mTemp.getRoot().getAbsolutePath();
    AgentListCatalogRequest r = AgentListCatalogRequest.newBuilder()
        .setConfDir(confDir)
        .build();
    Files.createDirectory(Paths.get(confDir, "catalog"));
    Path testCatalog = Paths.get(confDir, "catalog", "test-hive.properties");
    Path badCatalog = Paths.get(confDir, "catalog", "bad.poperties");
    Files.createFile(testCatalog);
    Files.write(testCatalog,
        "hive.metastore.uri=thrift://aaabbbccc::9083/\nconnector.name=hive-hadoop2\n".getBytes());
    Files.write(badCatalog,
        "hive.metastore.uri=thrift://aaabbbccc::9083/\nconnector.name=hive-hadoop2\n".getBytes());
    AgentListCatalogResponse resp = mClient.listCatalogs(r);
    assertEquals(1, resp.getCatalogCount());
    PrestoCatalogListing l = resp.getCatalog(0);
    assertEquals("test-hive", l.getCatalogName());
    assertEquals("thrift://aaabbbccc::9083/", l.getHiveMetastoreUri());
  }

  @Test
  public void testPrestoConfDirValidate() throws IOException {
    Path confDir = mTemp.getRoot().toPath().toAbsolutePath();
    Path nonexist = Paths.get("/tmp/path/to/nonexist/dir");
    Path nonabsolute = Paths.get("tmp/path/to/nonexist/dir");
    AgentValidatePrestoConfRequest.Builder pcr = AgentValidatePrestoConfRequest.newBuilder()
        .setCatalogName("na")
        .setPrestoConfDir(nonexist.toString());
    assertEquals(ValidationStatus.FAILED,
        mClient.validatePrestoConfDir(pcr.build()).getResult().getTestResult());

    pcr.setPrestoConfDir(nonabsolute.toString());
    assertEquals(ValidationStatus.FAILED,
        mClient.validatePrestoConfDir(pcr.build()).getResult().getTestResult());

    Path newFile = Files.createFile(confDir.resolve("testConfDir"));
    pcr.setPrestoConfDir(newFile.toString());
    assertEquals(ValidationStatus.FAILED,
        mClient.validatePrestoConfDir(pcr.build()).getResult().getTestResult());

    pcr.setPrestoConfDir(confDir.toString());
    assertEquals(ValidationStatus.OK,
        mClient.validatePrestoConfDir(pcr.build()).getResult().getTestResult());
  }
}
