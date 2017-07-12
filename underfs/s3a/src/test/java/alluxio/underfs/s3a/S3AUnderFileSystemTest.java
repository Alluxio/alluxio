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

package alluxio.underfs.s3a;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.PropertyKey;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for the {@link S3AUnderFileSystem}.
 */
public class S3AUnderFileSystemTest {
  private S3AUnderFileSystem mS3UnderFileSystem;
  private AmazonS3Client mClient;
  private TransferManager mManager;

  private static final String PATH = "path";
  private static final String SRC = "src";
  private static final String DST = "dst";

  private static final String BUCKET_NAME = "bucket";
  private static final String DEFAULT_OWNER = "";
  private static final short DEFAULT_MODE = 0700;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws InterruptedException, AmazonClientException {
    mClient = Mockito.mock(AmazonS3Client.class);
    mManager = Mockito.mock(TransferManager.class);
    mS3UnderFileSystem = new S3AUnderFileSystem(new AlluxioURI(""), mClient, BUCKET_NAME,
        mManager, UnderFileSystemConfiguration.defaults());
  }

  @Test
  public void deleteNonRecursiveOnAmazonClientException() throws IOException {
    Mockito.when(mClient.listObjectsV2(Matchers.any(ListObjectsV2Request.class)))
        .thenThrow(AmazonClientException.class);

    mThrown.expect(IOException.class);
    mS3UnderFileSystem.deleteDirectory(PATH, DeleteOptions.defaults().setRecursive(false));
  }

  @Test
  public void deleteRecursiveOnAmazonClientException() throws IOException {
    Mockito.when(mClient.listObjectsV2(Matchers.any(ListObjectsV2Request.class)))
        .thenThrow(AmazonClientException.class);

    mThrown.expect(IOException.class);
    mS3UnderFileSystem.deleteDirectory(PATH, DeleteOptions.defaults().setRecursive(true));
  }

  @Test
  public void renameOnAmazonClientException() throws IOException {
    Mockito.when(mClient.listObjectsV2(Matchers.any(ListObjectsV2Request.class)))
        .thenThrow(AmazonClientException.class);

    boolean result = mS3UnderFileSystem.renameFile(SRC, DST);
    Assert.assertFalse(result);
  }

  @Test
  public void createCredentialsFromConf() throws Exception {
    Map<PropertyKey, String> conf = new HashMap<>();
    conf.put(PropertyKey.S3A_ACCESS_KEY, "key1");
    conf.put(PropertyKey.S3A_SECRET_KEY, "key2");
    try (Closeable c = new ConfigurationRule(conf).toResource()) {
      UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults();
      AWSCredentialsProvider credentialsProvider =
          S3AUnderFileSystem.createAwsCredentialsProvider(ufsConf);
      Assert.assertEquals("key1", credentialsProvider.getCredentials().getAWSAccessKeyId());
      Assert.assertEquals("key2", credentialsProvider.getCredentials().getAWSSecretKey());
      Assert.assertTrue(credentialsProvider instanceof StaticCredentialsProvider);
    }
  }

  @Test
  public void createCredentialsFromDefault() throws Exception {
    UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults();
    AWSCredentialsProvider credentialsProvider =
        S3AUnderFileSystem.createAwsCredentialsProvider(ufsConf);
    Assert.assertTrue(credentialsProvider instanceof DefaultAWSCredentialsProviderChain);
  }

  @Test
  public void getPermissionsCached() throws Exception {
    Mockito.when(mClient.getS3AccountOwner()).thenReturn(new Owner("0", "test"));
    Mockito.when(mClient.getBucketAcl(Mockito.anyString())).thenReturn(new AccessControlList());
    mS3UnderFileSystem.getPermissions();
    mS3UnderFileSystem.getPermissions();
    Mockito.verify(mClient).getS3AccountOwner();
    Mockito.verify(mClient).getBucketAcl(Mockito.anyString());
  }

  @Test
  public void getPermissionsDefault() throws Exception {
    Mockito.when(mClient.getS3AccountOwner()).thenThrow(AmazonClientException.class);
    ObjectUnderFileSystem.ObjectPermissions permissions = mS3UnderFileSystem.getPermissions();
    Assert.assertEquals(DEFAULT_OWNER, permissions.getGroup());
    Assert.assertEquals(DEFAULT_OWNER, permissions.getOwner());
    Assert.assertEquals(DEFAULT_MODE, permissions.getMode());
  }
}
