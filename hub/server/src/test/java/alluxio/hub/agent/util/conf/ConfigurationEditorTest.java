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

package alluxio.hub.agent.util.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import alluxio.hub.proto.AlluxioConfigurationSet;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class ConfigurationEditorTest {
  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  private Path mSiteProps;
  private Path mEnvSh;
  private Path mLog4j;
  private Path mConfDir;

  @Before
  public void before() throws Exception {
    mConfDir = mTempFolder.getRoot().toPath();
    mSiteProps = mConfDir.resolve(ConfigurationEditor.SITE_PROPERTIES);
    mEnvSh = mConfDir.resolve(ConfigurationEditor.ENV_FILE);
    mLog4j = mConfDir.resolve(ConfigurationEditor.LOG_FILE);
    Files.createFile(mSiteProps);
    Files.createFile(mEnvSh);
    Files.createFile(mLog4j);
  }

  @Test
  public void testBasic() {
    List<String> paths = ImmutableList.of(mTempFolder.getRoot().getAbsolutePath(),
        "/nonexistent", "/tmp");
    ConfigurationEditor editor = new ConfigurationEditor(paths);
    assertEquals(mSiteProps, editor.getSiteProperties());
    assertEquals(mEnvSh, editor.getEnvSh());
    assertEquals(mLog4j, editor.getLogProps());

    paths = ImmutableList.of(mTempFolder.getRoot().getAbsolutePath());
    editor = new ConfigurationEditor(paths);
    assertEquals(mSiteProps, editor.getSiteProperties());
    assertEquals(mEnvSh, editor.getEnvSh());
    assertEquals(mLog4j, editor.getLogProps());
  }

  @Test
  public void testMissingSiteProp() throws IOException {
    List<String> paths = ImmutableList.of(mTempFolder.getRoot().getAbsolutePath());
    Files.delete(mSiteProps);
    ConfigurationEditor editor = new ConfigurationEditor(paths);
    assertNull(editor.getSiteProperties());
  }

  @Test
  public void testMissingEnvSh() throws IOException {
    List<String> paths = ImmutableList.of(mTempFolder.getRoot().getAbsolutePath());
    Files.delete(mEnvSh);
    ConfigurationEditor editor = new ConfigurationEditor(paths);
    assertNull(editor.getEnvSh());
  }

  @Test
  public void testMissingBoth() throws IOException {
    List<String> paths = ImmutableList.of(mTempFolder.getRoot().getAbsolutePath());
    Files.delete(mEnvSh);
    Files.delete(mSiteProps);
    ConfigurationEditor editor = new ConfigurationEditor(paths);
    assertNull(editor.getSiteProperties());
  }

  @Test
  public void testRead() throws IOException {
    String siteProps = "alluxio.test=true";
    String envSh = "ALLUXIO_JAVA_OPTS=\"-XX:DebugNonSafepoints\"";
    String log4j = "log4j.rootLogger=INFO, ${alluxio.logger.type}, ${alluxio.remote.logger.type}";
    Files.write(mSiteProps, siteProps.getBytes());
    Files.write(mEnvSh, envSh.getBytes());
    Files.write(mLog4j, log4j.getBytes());
    ConfigurationEditor editor = new ConfigurationEditor(
        ImmutableList.of(mConfDir.toAbsolutePath().toString()));
    AlluxioConfigurationSet s = editor.readConf();
    assertTrue(s.getSiteProperties().contains(siteProps));
    assertTrue(s.getAlluxioEnv().contains(envSh));
    assertTrue(s.getLog4JProperties().contains(log4j));
  }

  @Test
  public void testWrite() throws IOException {
    String siteProps = "alluxio.test=true";
    String envSh = "ALLUXIO_JAVA_OPTS=\"-XX:DebugNonSafepoints\"";
    String log4j = "log4j.rootLogger=INFO, ${alluxio.logger.type}, ${alluxio.remote.logger.type}";
    AlluxioConfigurationSet s = AlluxioConfigurationSet.newBuilder()
        .setSiteProperties(siteProps)
        .setAlluxioEnv(envSh)
        .setLog4JProperties(log4j)
        .build();
    ConfigurationEditor editor = new ConfigurationEditor(
        ImmutableList.of(mConfDir.toAbsolutePath().toString()));
    editor.writeConf(s);
    assertEquals(s, editor.readConf());
  }
}
