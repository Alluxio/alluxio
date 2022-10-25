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

package alluxio.master.meta;

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
  public void userAgentEnvironmentStringEmpty() throws Exception {
    String userAgentString = UpdateCheck.getUserAgentEnvironmentString("cluster1");
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));

    Assert.assertTrue(userAgentString.equals("cluster1"));
  }

  @Test
  public void userAgentEnvironmentStringDocker() throws Exception {
    Mockito.when(EnvironmentUtils.isDocker()).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));

    String userAgentString = UpdateCheck.getUserAgentEnvironmentString("cluster1");
    Assert.assertTrue(userAgentString.equals("cluster1; docker"));
  }

  @Test
  public void userAgentEnvironmentStringK8s() throws Exception {
    Mockito.when(EnvironmentUtils.isDocker()).thenReturn(true);
    Mockito.when(EnvironmentUtils.isKubernetes()).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));

    String userAgentString = UpdateCheck.getUserAgentEnvironmentString("cluster1");
    Assert.assertTrue(userAgentString.equals("cluster1; docker; kubernetes"));
  }

  @Test
  public void userAgentEnvironmentStringGCP() throws Exception {
    Mockito.when(EnvironmentUtils.isGoogleComputeEngine()).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData())
        .thenThrow(new SdkClientException("Unable to contact EC2 metadata service."));

    String userAgentString = UpdateCheck.getUserAgentEnvironmentString("cluster1");
    Assert.assertTrue(userAgentString.equals("cluster1; gce"));
  }

  @Test
  public void userAgentEnvironmentStringEC2AMI() throws Exception {
    Mockito.when(EnvironmentUtils.isEC2()).thenReturn(true);
    Mockito.when(EnvironmentUtils.getEC2ProductCode()).thenReturn("random123code");
    // When no user data in this ec2, null is returned
    Mockito.when(EC2MetadataUtils.getUserData()).thenReturn(null);

    String userAgentString = UpdateCheck.getUserAgentEnvironmentString("cluster1");
    Assert.assertTrue(userAgentString.equals("cluster1; ProductCode:random123code; ec2"));
  }

  @Test
  public void userAgentEnvironmentStringEC2CFT() throws Exception {
    Mockito.when(EnvironmentUtils.isEC2()).thenReturn(true);
    Mockito.when(EnvironmentUtils.getEC2ProductCode()).thenReturn("random123code");
    Mockito.when(EnvironmentUtils.isCFT(Mockito.anyString())).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData()).thenReturn("{ \"cft_configure\": {}}");

    String userAgentString = UpdateCheck.getUserAgentEnvironmentString("cluster1");
    Assert.assertTrue(userAgentString.equals("cluster1; ProductCode:random123code; cft; ec2"));
  }

  @Test
  public void userAgentEnvironmentStringEC2EMR() throws Exception {
    Mockito.when(EnvironmentUtils.isEC2()).thenReturn(true);
    Mockito.when(EnvironmentUtils.getEC2ProductCode()).thenReturn("random123code");
    Mockito.when(EnvironmentUtils.isEMR(Mockito.anyString())).thenReturn(true);
    Mockito.when(EC2MetadataUtils.getUserData()).thenReturn("emr_apps");

    String userAgentString = UpdateCheck.getUserAgentEnvironmentString("cluster1");
    Assert.assertTrue(userAgentString.equals("cluster1; ProductCode:random123code; emr; ec2"));
  }

  @Test
  public void featureStringEmbeddedJournal() {
    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS);
    Assert.assertFalse(UpdateCheck.getUserAgentFeatureList().contains("embedded"));
    Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED);
    Assert.assertTrue(UpdateCheck.getUserAgentFeatureList().contains("embedded"));
  }

  @Test
  public void featureStringRocks() {
    Configuration.set(PropertyKey.MASTER_METASTORE, MetastoreType.ROCKS);
    Assert.assertTrue(UpdateCheck.getUserAgentFeatureList().contains("rocks"));
    Configuration.set(PropertyKey.MASTER_METASTORE, MetastoreType.HEAP);
    Assert.assertFalse(UpdateCheck.getUserAgentFeatureList().contains("rocks"));
  }

  @Test
  public void featureStringZookeeper() {
    Configuration.set(PropertyKey.ZOOKEEPER_ENABLED, true);
    Assert.assertTrue(UpdateCheck.getUserAgentFeatureList().contains("zk"));
    Configuration.set(PropertyKey.ZOOKEEPER_ENABLED, false);
    Assert.assertFalse(UpdateCheck.getUserAgentFeatureList().contains("zk"));
  }

  @Test
  public void featureStringBackupDelegation() {
    Configuration.set(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, true);
    Assert.assertTrue(UpdateCheck.getUserAgentFeatureList().contains("backupDelegation"));
    Configuration.set(PropertyKey.MASTER_BACKUP_DELEGATION_ENABLED, false);
    Assert.assertFalse(UpdateCheck.getUserAgentFeatureList().contains("backupDelegation"));
  }

  @Test
  public void featureStringDailyBackup() {
    Configuration.set(PropertyKey.MASTER_DAILY_BACKUP_ENABLED, true);
    Assert.assertTrue(UpdateCheck.getUserAgentFeatureList().contains("dailyBackup"));
    Configuration.set(PropertyKey.MASTER_DAILY_BACKUP_ENABLED, false);
    Assert.assertFalse(UpdateCheck.getUserAgentFeatureList().contains("dailyBackup"));
  }

  @Test
  public void featureStringPersistneceBlacklist() {
    Configuration.set(PropertyKey.MASTER_PERSISTENCE_BLACKLIST, ".tmp");
    Assert.assertTrue(UpdateCheck.getUserAgentFeatureList().contains("persistBlackList"));
    Configuration.unset(PropertyKey.MASTER_PERSISTENCE_BLACKLIST);
    Assert.assertFalse(UpdateCheck.getUserAgentFeatureList().contains("persistBlackList"));
  }

  @Test
  public void featureStringUnsafePersist() {
    Configuration.set(PropertyKey.MASTER_UNSAFE_DIRECT_PERSIST_OBJECT_ENABLED, true);
    Assert.assertTrue(UpdateCheck.getUserAgentFeatureList().contains("unsafePersist"));
    Configuration.set(PropertyKey.MASTER_UNSAFE_DIRECT_PERSIST_OBJECT_ENABLED, false);
    Assert.assertFalse(UpdateCheck.getUserAgentFeatureList().contains("unsafePersist"));
  }

  @Test
  public void featureStringMasterAuditLogging() {
    Configuration.set(PropertyKey.MASTER_AUDIT_LOGGING_ENABLED, true);
    Assert.assertTrue(UpdateCheck.getUserAgentFeatureList().contains("masterAuditLog"));
    Configuration.set(PropertyKey.MASTER_AUDIT_LOGGING_ENABLED, false);
    Assert.assertFalse(UpdateCheck.getUserAgentFeatureList().contains("masterAuditLog"));
  }

  @Test
  public void featureStringPageStore() {
    Configuration.set(PropertyKey.WORKER_BLOCK_STORE_TYPE, BlockStoreType.PAGE);
    Assert.assertTrue(UpdateCheck.getUserAgentFeatureList().contains("pageStore"));
    Configuration.set(PropertyKey.WORKER_BLOCK_STORE_TYPE, BlockStoreType.FILE);
    Assert.assertFalse(UpdateCheck.getUserAgentFeatureList().contains("pageStore"));
  }

  @Test
  public void userAgentClusterInfoList() {
    Assert.assertTrue(UpdateCheck.getUserAgentClusterInfoList(1).contains("numWorkers:1"));
    Assert.assertTrue(UpdateCheck.getUserAgentClusterInfoList(0).contains("numWorkers:-1"));
  }

  @Test
  public void userAgent() {
    String userAgentString = UpdateCheck.getUserAgentString("cluster1", 1);
    Pattern pattern = Pattern.compile(
        String.format("Alluxio\\/%s \\(cluster1(?:.+)[^;]\\)", ProjectConstants.VERSION));
    Matcher matcher = pattern.matcher(userAgentString);
    Assert.assertTrue(matcher.matches());
  }
}
