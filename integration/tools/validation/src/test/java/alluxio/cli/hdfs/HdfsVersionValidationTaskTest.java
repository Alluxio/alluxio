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

package alluxio.cli.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.core.StringContains.containsString;

import alluxio.cli.ValidationTaskResult;
import alluxio.cli.ValidationUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ShellUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ShellUtils.class)
public class HdfsVersionValidationTaskTest {
  private static InstancedConfiguration sConf;

  @BeforeClass
  public static void initConf() throws IOException {
    sConf = InstancedConfiguration.defaults();
  }

  @Test
  public void versionNotMatchedDefault() throws Exception {
    PowerMockito.mockStatic(ShellUtils.class);
    String[] cmd = new String[]{"hadoop", "version"};
    BDDMockito.given(ShellUtils.execCommand(cmd)).willReturn("Hadoop 2.2");

    HdfsVersionValidationTask task = new HdfsVersionValidationTask(sConf);
    ValidationTaskResult result = task.validateImpl(ImmutableMap.of());
    assertEquals(ValidationUtils.State.FAILED, result.getState());
    assertThat(result.getResult(), containsString("2.2 does not match alluxio.underfs.version"));
    assertThat(result.getAdvice(), containsString("configure alluxio.underfs.version"));
  }

  @Test
  public void versionNotMatched() throws Exception {
    PowerMockito.mockStatic(ShellUtils.class);
    String[] cmd = new String[]{"hadoop", "version"};
    BDDMockito.given(ShellUtils.execCommand(cmd)).willReturn("Hadoop 2.7");
    sConf.set(PropertyKey.UNDERFS_VERSION, "2.6");

    HdfsVersionValidationTask task = new HdfsVersionValidationTask(sConf);
    ValidationTaskResult result = task.validateImpl(ImmutableMap.of());
    assertEquals(ValidationUtils.State.FAILED, result.getState());
    assertThat(result.getResult(), containsString(
            "2.7 does not match alluxio.underfs.version=2.6"));
    assertThat(result.getAdvice(), containsString("configure alluxio.underfs.version"));
  }

  @Test
  public void versionMatched() throws Exception {
    PowerMockito.mockStatic(ShellUtils.class);
    String[] cmd = new String[]{"hadoop", "version"};
    BDDMockito.given(ShellUtils.execCommand(cmd)).willReturn("Hadoop 2.6");
    sConf.set(PropertyKey.UNDERFS_VERSION, "2.6");

    HdfsVersionValidationTask task = new HdfsVersionValidationTask(sConf);
    ValidationTaskResult result = task.validateImpl(ImmutableMap.of());
    assertEquals(ValidationUtils.State.OK, result.getState());
  }

  @Test
  public void minorVersionAccepted() throws Exception {
    PowerMockito.mockStatic(ShellUtils.class);
    String[] cmd = new String[]{"hadoop", "version"};
    // The minor version is not defined in Alluxio, which should work
    BDDMockito.given(ShellUtils.execCommand(cmd)).willReturn("Hadoop 2.6.2");
    sConf.set(PropertyKey.UNDERFS_VERSION, "2.6");

    HdfsVersionValidationTask task = new HdfsVersionValidationTask(sConf);
    ValidationTaskResult result = task.validateImpl(ImmutableMap.of());
    assertEquals(ValidationUtils.State.OK, result.getState());
  }

  @Test
  public void minorVersionConflict() throws Exception {
    PowerMockito.mockStatic(ShellUtils.class);
    String[] cmd = new String[]{"hadoop", "version"};
    // Alluxio defines a different minor version, which should not work
    BDDMockito.given(ShellUtils.execCommand(cmd)).willReturn("Hadoop 2.6.2");
    sConf.set(PropertyKey.UNDERFS_VERSION, "2.6.3");

    HdfsVersionValidationTask task = new HdfsVersionValidationTask(sConf);
    ValidationTaskResult result = task.validateImpl(ImmutableMap.of());
    assertEquals(ValidationUtils.State.FAILED, result.getState());
    assertThat(result.getResult(), containsString(
            "Hadoop version 2.6.2 does not match alluxio.underfs.version=2.6.3"));
  }

  @Test
  public void versionParsing() {
    String versionStr = "Hadoop 2.7.2\n"
            + "Subversion https://git-wip-us.apache.org/repos/asf/hadoop.git "
            + "-r b165c4fe8a74265c792ce23f546c64604acf0e41\n"
            + "Compiled by jenkins on 2016-01-26T00:08Z\n"
            + "Compiled with protoc 2.5.0\n"
            + "From source with checksum d0fda26633fa762bff87ec759ebe689c\n"
            + "This command was run using "
            + "/tmp/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar";

    HdfsVersionValidationTask task = new HdfsVersionValidationTask(sConf);
    String version = task.parseVersion(versionStr);
    assertEquals("2.7.2", version);
  }

  @Test
  public void cdhVersionParsing() {
    String versionStr = "Hadoop 2.6.0-cdh5.16.2\n"
            + "Subversion http://github.com/cloudera/hadoop -r "
            + "4f94d60caa4cbb9af0709a2fd96dc3861af9cf20\n"
            + "Compiled by jenkins on 2019-06-03T10:41Z\n"
            + "Compiled with protoc 2.5.0\n"
            + "From source with checksum 79b9b24a29c6358b53597c3b49575e37\n"
            + "This command was run using /usr/lib/hadoop/hadoop-common-2.6.0-cdh5.16.2.jar";

    HdfsVersionValidationTask task = new HdfsVersionValidationTask(sConf);
    String version = task.parseVersion(versionStr);
    assertEquals("cdh5.16.2", version);
  }

  @After
  public void resetConfig() {
    sConf.unset(PropertyKey.UNDERFS_VERSION);
  }
}
