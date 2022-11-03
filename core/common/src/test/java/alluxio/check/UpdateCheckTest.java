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

package alluxio.check;

import alluxio.ProjectConstants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.journal.JournalType;
import alluxio.master.metastore.MetastoreType;
import alluxio.util.EnvironmentUtils;
import alluxio.worker.block.BlockStoreType;

import com.amazonaws.SdkClientException;
import com.amazonaws.util.EC2MetadataUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Unit tests for {@link UpdateCheck}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({EnvironmentUtils.class, EC2MetadataUtils.class})
public class UpdateCheckTest {

  @Before
  public void before() {
    PowerMockito.mockStatic(EnvironmentUtils.class);
    Mockito.when(EnvironmentUtils.isDocker()).thenReturn(false);
    Mockito.when(EnvironmentUtils.isKubernetes()).thenReturn(false);
    Mockito.when(EnvironmentUtils.isGoogleComputeEngine()).thenReturn(false);
    Mockito.when(EnvironmentUtils.getEC2ProductCode()).thenReturn("");
    Mockito.when(EnvironmentUtils.isEC2()).thenReturn(false);
    Mockito.when(EnvironmentUtils.isCFT(Mockito.anyString())).thenReturn(false);
    Mockito.when(EnvironmentUtils.isEMR(Mockito.anyString())).thenReturn(false);
    PowerMockito.mockStatic(EC2MetadataUtils.class);
  }

  @Test
  public void userAgentEnvironmentStringEmpty() {
    List<String> info = new ArrayList<>();
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));
    UpdateCheck.addUserAgentEnvironments(info);
    Assert.assertEquals(0, info.size());
  }

  @Test
  public void userAgentEnvironmentStringDocker() {
    Mockito.when(EnvironmentUtils.isDocker()).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));
    List<String> info = new ArrayList<>();
    UpdateCheck.addUserAgentEnvironments(info);
    Assert.assertEquals(1, info.size());
    Assert.assertEquals(UpdateCheck.DOCKER_KEY, info.get(0));
  }

  @Test
  public void userAgentEnvironmentStringK8s() {
    Mockito.when(EnvironmentUtils.isDocker()).thenReturn(true);
    Mockito.when(EnvironmentUtils.isKubernetes()).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));
    List<String> info = new ArrayList<>();
    UpdateCheck.addUserAgentEnvironments(info);
    Assert.assertEquals(2, info.size());
    Assert.assertEquals(UpdateCheck.DOCKER_KEY, info.get(0));
    Assert.assertEquals(UpdateCheck.KUBERNETES_KEY, info.get(1));
  }

  @Test
  public void userAgentEnvironmentStringGCP() {
    Mockito.when(EnvironmentUtils.isGoogleComputeEngine()).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));
    List<String> info = new ArrayList<>();
    UpdateCheck.addUserAgentEnvironments(info);
    Assert.assertEquals(1, info.size());
    Assert.assertEquals(UpdateCheck.GCE_KEY, info.get(0));
  }

  @Test
  public void userAgentEnvironmentStringEC2AMI() {
    String randomProductCode = "random123code";
    Mockito.when(EnvironmentUtils.isEC2()).thenReturn(true);
    Mockito.when(EnvironmentUtils.getEC2ProductCode()).thenReturn(randomProductCode);
    // When no user data in this ec2, null is returned
    Mockito.when(EC2MetadataUtils.getUserData()).thenReturn(null);
    List<String> info = new ArrayList<>();
    UpdateCheck.addUserAgentEnvironments(info);
    Assert.assertEquals(2, info.size());
    Assert.assertEquals(String.format(UpdateCheck.PRODUCT_CODE_FORMAT, randomProductCode),
        info.get(0));
    Assert.assertEquals(UpdateCheck.EC2_KEY, info.get(1));
  }

  @Test
  public void userAgentEnvironmentStringEC2CFT() {
    String randomProductCode = "random123code";
    Mockito.when(EnvironmentUtils.isEC2()).thenReturn(true);
    Mockito.when(EnvironmentUtils.getEC2ProductCode()).thenReturn(randomProductCode);
    Mockito.when(EnvironmentUtils.isCFT(Mockito.anyString())).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData()).thenReturn("{ \"cft_configure\": {}}");

    List<String> info = new ArrayList<>();
    UpdateCheck.addUserAgentEnvironments(info);
    Assert.assertEquals(3, info.size());
    Assert.assertEquals(String.format(UpdateCheck.PRODUCT_CODE_FORMAT, randomProductCode),
        info.get(0));
    Assert.assertEquals(UpdateCheck.CFT_KEY, info.get(1));
    Assert.assertEquals(UpdateCheck.EC2_KEY, info.get(2));
  }

  @Test
  public void userAgentEnvironmentStringEC2EMR() {
    String randomProductCode = "random123code";
    Mockito.when(EnvironmentUtils.isEC2()).thenReturn(true);
    Mockito.when(EnvironmentUtils.getEC2ProductCode()).thenReturn(randomProductCode);
    Mockito.when(EnvironmentUtils.isEMR(Mockito.anyString())).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData()).thenReturn("emr_apps");
    List<String> info = new ArrayList<>();
    UpdateCheck.addUserAgentEnvironments(info);
    Assert.assertEquals(3, info.size());
    Assert.assertEquals(String.format(UpdateCheck.PRODUCT_CODE_FORMAT, randomProductCode),
        info.get(0));
    Assert.assertEquals(UpdateCheck.EMR_KEY, info.get(1));
    Assert.assertEquals(UpdateCheck.EC2_KEY, info.get(2));
  }

  @Test
  public void featureStringEmbeddedJournal() {
    List<String> info = new ArrayList<>();
    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS);
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertFalse(listContainsTarget(info, UpdateCheck.EMBEDDED_KEY));
    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED);
    info.clear();
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertTrue(listContainsTarget(info, UpdateCheck.EMBEDDED_KEY));
  }

  @Test
  public void featureStringInodeMetastoreRocks() {
    List<String> info = new ArrayList<>();
    Configuration.set(PropertyKey.MASTER_INODE_METASTORE, MetastoreType.ROCKS);
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertTrue(listContainsTarget(info, UpdateCheck.INODE_METASTORE_ROCKS_KEY));
    Configuration.set(PropertyKey.MASTER_INODE_METASTORE, MetastoreType.HEAP);
    info.clear();
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertFalse(listContainsTarget(info, UpdateCheck.INODE_METASTORE_ROCKS_KEY));
  }

  @Test
  public void featureStringBlockMetastoreRocks() {
    List<String> info = new ArrayList<>();
    Configuration.set(PropertyKey.MASTER_BLOCK_METASTORE, MetastoreType.ROCKS);
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertTrue(listContainsTarget(info, UpdateCheck.BLOCK_METASTORE_ROCKS_KEY));
    Configuration.set(PropertyKey.MASTER_BLOCK_METASTORE, MetastoreType.HEAP);
    info.clear();
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertFalse(listContainsTarget(info, UpdateCheck.BLOCK_METASTORE_ROCKS_KEY));
  }

  @Test
  public void featureStringZookeeper() {
    List<String> info = new ArrayList<>();
    Configuration.set(PropertyKey.ZOOKEEPER_ENABLED, true);
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertTrue(listContainsTarget(info, UpdateCheck.ZOOKEEPER_KEY));
    Configuration.set(PropertyKey.ZOOKEEPER_ENABLED, false);
    info.clear();
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertFalse(listContainsTarget(info, UpdateCheck.ZOOKEEPER_KEY));
  }

  @Test
  public void featureStringBackupDelegation() {
    List<String> info = new ArrayList<>();
    Configuration.set(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, true);
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertTrue(listContainsTarget(info, UpdateCheck.BACKUP_DELEGATION_KEY));
    Configuration.set(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, false);
    info.clear();
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertFalse(listContainsTarget(info, UpdateCheck.BACKUP_DELEGATION_KEY));
  }

  @Test
  public void featureStringDailyBackup() {
    List<String> info = new ArrayList<>();
    Configuration.set(PropertyKey.MASTER_DAILY_BACKUP_ENABLED, true);
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertTrue(listContainsTarget(info, UpdateCheck.DAILY_BACKUP_KEY));
    Configuration.set(PropertyKey.MASTER_DAILY_BACKUP_ENABLED, false);
    info.clear();
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertFalse(listContainsTarget(info, UpdateCheck.DAILY_BACKUP_KEY));
  }

  @Test
  public void featureStringPersistneceBlacklist() {
    List<String> info = new ArrayList<>();
    Configuration.set(PropertyKey.MASTER_PERSISTENCE_BLACKLIST, ".tmp");
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertTrue(listContainsTarget(info, UpdateCheck.PERSIST_BLACK_LIST_KEY));
    Configuration.unset(PropertyKey.MASTER_PERSISTENCE_BLACKLIST);
    info.clear();
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertFalse(listContainsTarget(info, UpdateCheck.PERSIST_BLACK_LIST_KEY));
  }

  @Test
  public void featureStringUnsafePersist() {
    List<String> info = new ArrayList<>();
    Configuration.set(PropertyKey.MASTER_UNSAFE_DIRECT_PERSIST_OBJECT_ENABLED, true);
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertTrue(listContainsTarget(info, UpdateCheck.UNSAFE_PERSIST_KEY));
    Configuration.set(PropertyKey.MASTER_UNSAFE_DIRECT_PERSIST_OBJECT_ENABLED, false);
    info.clear();
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertFalse(listContainsTarget(info, UpdateCheck.UNSAFE_PERSIST_KEY));
  }

  @Test
  public void featureStringMasterAuditLogging() {
    List<String> info = new ArrayList<>();
    Configuration.set(PropertyKey.MASTER_AUDIT_LOGGING_ENABLED, true);
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertTrue(listContainsTarget(info, UpdateCheck.MASTER_AUDIT_LOG_KEY));
    Configuration.set(PropertyKey.MASTER_AUDIT_LOGGING_ENABLED, false);
    info.clear();
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertFalse(listContainsTarget(info, UpdateCheck.MASTER_AUDIT_LOG_KEY));
  }

  @Test
  public void featureStringPageStore() {
    List<String> info = new ArrayList<>();
    Configuration.set(PropertyKey.WORKER_BLOCK_STORE_TYPE, BlockStoreType.PAGE);
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertTrue(listContainsTarget(info, UpdateCheck.PAGE_STORE_KEY));
    Configuration.set(PropertyKey.WORKER_BLOCK_STORE_TYPE, BlockStoreType.FILE);
    info.clear();
    UpdateCheck.addUserAgentFeatures(info);
    Assert.assertFalse(listContainsTarget(info, UpdateCheck.PAGE_STORE_KEY));
  }

  @Test
  public void userAgent() {
    String userAgentString = UpdateCheck.getUserAgentString("cluster1", new ArrayList<>());
    Pattern pattern = Pattern.compile(
        String.format("Alluxio\\/%s \\(cluster1(?:.+)[^;]\\)", ProjectConstants.VERSION));
    Matcher matcher = pattern.matcher(userAgentString);
    Assert.assertTrue(matcher.matches());
  }

  /**
   * Makes sure the list containing the target information.
   *
   * @param list the list to check
   * @param target the target info
   * @return true if list contains the target
   */
  private boolean listContainsTarget(List<String> list, String target) {
    for (String str : list) {
      if (str.equals(target)) {
        return true;
      }
    }
    return false;
  }
}
