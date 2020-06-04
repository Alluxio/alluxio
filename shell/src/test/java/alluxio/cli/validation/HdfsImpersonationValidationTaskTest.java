package alluxio.cli.validation;

import alluxio.cli.ValidateUtils;
import alluxio.cli.bundler.InfoCollectorTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ShellUtils;
import alluxio.util.network.NetworkAddressUtils;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(PowerMockRunner.class)
@PrepareForTest(NetworkAddressUtils.class)
public class HdfsImpersonationValidationTaskTest {
  private static File sTestDir;
  private InstancedConfiguration mConf;

  @BeforeClass
  public static void prepareConfDir() throws IOException {
    sTestDir = HdfsConfValidationTaskTest.prepareConfDir();
  }

  @Before
  public void prepareConf() {
    mConf = InstancedConfiguration.defaults();
    mConf.set(PropertyKey.CONF_DIR, sTestDir.getAbsolutePath());
  }

  public void prepareHdfsConfFiles(Map<String, String> coreSiteProps) {
    String coreSite = Paths.get(sTestDir.toPath().toString(), "core-site.xml").toString();
    HdfsConfValidationTaskTest.writeXML(coreSite, coreSiteProps);

    String hdfsSite = Paths.get(sTestDir.toPath().toString(), "hdfs-site.xml").toString();
    HdfsConfValidationTaskTest.writeXML(hdfsSite, ImmutableMap.of("key2", "value2"));
  }

  @Test
  public void skipped() {
    HdfsImpersonationValidationTask task =
            new HdfsImpersonationValidationTask("hdfs://namenode:9000/alluxio", mConf, HdfsImpersonationValidationTask.Mode.USERS);
    ValidateUtils.TaskResult result = task.validate(ImmutableMap.of());
    System.out.println(result);
    assertEquals(ValidateUtils.State.SKIPPED, result.getState());
  }

  @Test
  public void missingProxyUser() {
    PropertyKey bobKey = PropertyKey.Template.MASTER_IMPERSONATION_USERS_OPTION.format("bob");
    mConf.set(bobKey, "*");

    // No proxy user definition in core-site.xml
    prepareHdfsConfFiles(ImmutableMap.of("key1", "value1"));

    HdfsImpersonationValidationTask task =
            new HdfsImpersonationValidationTask("hdfs://namenode:9000/alluxio", mConf, HdfsImpersonationValidationTask.Mode.USERS);
    ValidateUtils.TaskResult result = task.validate(ImmutableMap.of());
    System.out.println(result);
    assertEquals(ValidateUtils.State.FAILED, result.getState());
    assertThat(result.getResult(), containsString("hadoop.proxy.bob.users is not configured"));
    assertThat(result.getAdvice(), containsString("Please configure hadoop.proxy.bob.users to match alluxio.master.security.impersonation.bob.users"));
  }

  @Test
  public void mismatchedProxyUserWildcard() {
    PropertyKey bobKey = PropertyKey.Template.MASTER_IMPERSONATION_USERS_OPTION.format("bob");
    mConf.set(bobKey, "*");

    // No proxy user definition in core-site.xml
    prepareHdfsConfFiles(ImmutableMap.of("hadoop.proxyuser.bob.users", "user1,user2"));

    HdfsImpersonationValidationTask task =
            new HdfsImpersonationValidationTask("hdfs://namenode:9000/alluxio", mConf, HdfsImpersonationValidationTask.Mode.USERS);
    ValidateUtils.TaskResult result = task.validate(ImmutableMap.of());
    System.out.println(result);
    assertEquals(ValidateUtils.State.FAILED, result.getState());
    assertThat(result.getResult(), containsString("User bob can impersonate any user in Alluxio but only user1,user2 in HDFS."));
    assertThat(result.getAdvice(), containsString("Please Please set hadoop.proxy.bob.users to *."));
  }

  @Test
  public void hdfsHasMoreProxyUsers() {
    PropertyKey bobKey = PropertyKey.Template.MASTER_IMPERSONATION_USERS_OPTION.format("bob");
    mConf.set(bobKey, "user1");

    // No proxy user definition in core-site.xml
    prepareHdfsConfFiles(ImmutableMap.of("hadoop.proxy.bob.users", "user1,user2"));

    HdfsImpersonationValidationTask task =
            new HdfsImpersonationValidationTask("hdfs://namenode:9000/alluxio", mConf, HdfsImpersonationValidationTask.Mode.USERS);
    ValidateUtils.TaskResult result = task.validate(ImmutableMap.of());
    System.out.println(result);
    assertEquals(ValidateUtils.State.OK, result.getState());
    assertThat(result.getResult(), containsString("All impersonable users in Alluxio are found in HDFS."));
  }

  @Test
  public void missingProxyUsersInHdfs() {
    PropertyKey bobKey = PropertyKey.Template.MASTER_IMPERSONATION_USERS_OPTION.format("bob");
    mConf.set(bobKey, "user1,user2");

    // No proxy user definition in core-site.xml
    prepareHdfsConfFiles(ImmutableMap.of("hadoop.proxyuser.bob.users", "user1,user3"));

    HdfsImpersonationValidationTask task =
            new HdfsImpersonationValidationTask("hdfs://namenode:9000/alluxio", mConf, HdfsImpersonationValidationTask.Mode.USERS);
    ValidateUtils.TaskResult result = task.validate(ImmutableMap.of());
    System.out.println(result);
    assertEquals(ValidateUtils.State.FAILED, result.getState());
    assertThat(result.getResult(), containsString("User bob can impersonate as users [user2] in Alluxio but not in HDFS."));
    assertThat(result.getAdvice(), containsString("Please add the missing users to hadoop.proxy.bob.users. "));
  }

  @Test
  public void wildcardProxyUserInHdfs() {
    PropertyKey bobKey = PropertyKey.Template.MASTER_IMPERSONATION_USERS_OPTION.format("bob");
    mConf.set(bobKey, "user1,user2,user3");

    // No proxy user definition in core-site.xml
    prepareHdfsConfFiles(ImmutableMap.of("hadoop.proxyuser.bob.users", "*"));

    HdfsImpersonationValidationTask task =
            new HdfsImpersonationValidationTask("hdfs://namenode:9000/alluxio", mConf, HdfsImpersonationValidationTask.Mode.USERS);
    ValidateUtils.TaskResult result = task.validate(ImmutableMap.of());
    System.out.println(result);
    assertEquals(ValidateUtils.State.OK, result.getState());
  }

  // TODO(jiacheng): add groups validation

  @Test
  public void canImpersonateFromHost() {
    PropertyKey bobKey = PropertyKey.Template.MASTER_IMPERSONATION_USERS_OPTION.format("bob");
    mConf.set(bobKey, "user1,user2,user3");

    PowerMockito.mockStatic(NetworkAddressUtils.class);
    BDDMockito.given(NetworkAddressUtils.getLocalHostName(1000)).willReturn("localhost");

    // No proxy user definition in core-site.xml
    prepareHdfsConfFiles(ImmutableMap.of("hadoop.proxyuser.bob.hosts", "localhost"));

    HdfsImpersonationValidationTask task =
            new HdfsImpersonationValidationTask("hdfs://namenode:9000/alluxio", mConf, HdfsImpersonationValidationTask.Mode.HOSTS);
    ValidateUtils.TaskResult result = task.validate(ImmutableMap.of());
    System.out.println(result);
    assertEquals(ValidateUtils.State.OK, result.getState());
  }

  @Test
  public void canImpersonateFromWildcard() {
    PropertyKey bobKey = PropertyKey.Template.MASTER_IMPERSONATION_USERS_OPTION.format("bob");
    mConf.set(bobKey, "user1,user2,user3");

    PowerMockito.mockStatic(NetworkAddressUtils.class);
    BDDMockito.given(NetworkAddressUtils.getLocalHostName(1000)).willReturn("localhost");

    // No proxy user definition in core-site.xml
    prepareHdfsConfFiles(ImmutableMap.of("hadoop.proxyuser.bob.hosts", "*"));

    HdfsImpersonationValidationTask task =
            new HdfsImpersonationValidationTask("hdfs://namenode:9000/alluxio", mConf, HdfsImpersonationValidationTask.Mode.HOSTS);
    ValidateUtils.TaskResult result = task.validate(ImmutableMap.of());
    System.out.println(result);
    assertEquals(ValidateUtils.State.OK, result.getState());
  }

  @Test
  public void cannotImpersonateFromHost() {
    PropertyKey bobKey = PropertyKey.Template.MASTER_IMPERSONATION_USERS_OPTION.format("bob");
    mConf.set(bobKey, "user1,user2,user3");

    PowerMockito.mockStatic(NetworkAddressUtils.class);
    BDDMockito.given(NetworkAddressUtils.getLocalHostName(1000)).willReturn("localhost");

    // No proxy user definition in core-site.xml
    prepareHdfsConfFiles(ImmutableMap.of("hadoop.proxyuser.bob.hosts", "host1"));

    HdfsImpersonationValidationTask task =
            new HdfsImpersonationValidationTask("hdfs://namenode:9000/alluxio", mConf, HdfsImpersonationValidationTask.Mode.HOSTS);
    ValidateUtils.TaskResult result = task.validate(ImmutableMap.of());
    System.out.println(result);
    assertEquals(ValidateUtils.State.FAILED, result.getState());
    assertThat(result.getResult(), containsString("hadoop.proxyuser.bob.hosts does not contain host localhost"));
    assertThat(result.getAdvice(), containsString("Please enable host localhost in hadoop.proxyuser.bob.hosts"));
  }
}
