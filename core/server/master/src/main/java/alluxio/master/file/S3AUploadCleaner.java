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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.util.UnderFileSystemUtils;
import alluxio.wire.MountPointInfo;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * S3A intermediate multipart upload periodic clean.
 */
@NotThreadSafe
final class S3AUploadCleaner implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(S3AUploadCleaner.class);
  private static final long CLEAN_AGE =
      Configuration.getMs(PropertyKey.UNDERFS_S3A_INTERMEDIATE_UPLOAD_CLEAN_AGE);

  private final FileSystemMaster mFileSystemMaster;
  private final ClientConfiguration mClientConf;

  /**
   * Constructs a new {@link S3AUploadCleaner}.
   */
  public S3AUploadCleaner(FileSystemMaster fileSystemMaster) {
    mFileSystemMaster = fileSystemMaster;

    // Set the client configuration based on Alluxio configuration values.
    mClientConf = new ClientConfiguration();

    // HTTP protocol
    if (Boolean.parseBoolean(Configuration.get(PropertyKey.UNDERFS_S3A_SECURE_HTTP_ENABLED))) {
      mClientConf.setProtocol(Protocol.HTTPS);
    } else {
      mClientConf.setProtocol(Protocol.HTTP);
    }

    // Proxy host
    if (Configuration.isSet(PropertyKey.UNDERFS_S3_PROXY_HOST)) {
      mClientConf.setProxyHost(Configuration.get(PropertyKey.UNDERFS_S3_PROXY_HOST));
    }

    // Proxy port
    if (Configuration.isSet(PropertyKey.UNDERFS_S3_PROXY_PORT)) {
      mClientConf.setProxyPort(Integer.parseInt(Configuration
          .get(PropertyKey.UNDERFS_S3_PROXY_PORT)));
    }

    // Signer algorithm
    if (Configuration.isSet(PropertyKey.UNDERFS_S3A_SIGNER_ALGORITHM)) {
      mClientConf.setSignerOverride(Configuration.get(PropertyKey.UNDERFS_S3A_SIGNER_ALGORITHM));
    }

    mClientConf.setRequestTimeout((int) Configuration
        .getMs(PropertyKey.UNDERFS_S3A_REQUEST_TIMEOUT));
    mClientConf.setSocketTimeout((int) Configuration
        .getMs(PropertyKey.UNDERFS_S3A_SOCKET_TIMEOUT_MS));
  }

  @Override
  public void heartbeat() {
    List<MountPointInfo> toCleanList = mFileSystemMaster.getMountTable().values().stream()
        .filter(info -> info.getUfsType().equals("s3") && !info.getReadOnly())
        .collect(Collectors.toList());
    for (MountPointInfo info : toCleanList) {
      cleanIntermediateUpload(info);
    }
  }

  @Override
  public void close() {
    // Nothing to clean up
  }

  /**
   * Cleans the intermediate multipart uploads in a mount point.
   *
   * @param info the information of this mount point
   */
  private void cleanIntermediateUpload(MountPointInfo info) {
    AWSCredentialsProvider credentials = createAwsCredentialsProvider(info.getProperties());
    AmazonS3 client = AmazonS3ClientBuilder.standard().withCredentials(credentials)
        .withClientConfiguration(mClientConf).build();
    TransferManager manager = TransferManagerBuilder.standard().withS3Client(client).build();

    String bucketName = UnderFileSystemUtils.getBucketName(new AlluxioURI(info.getUfsUri()));
    Date cleanBefore = new Date(new Date().getTime() - CLEAN_AGE);
    try {
      manager.abortMultipartUploads(bucketName, cleanBefore);
      LOG.info("Cleaned intermediate multipart uploads in bucket: {}", info.getUfsUri());
    } catch (Exception e) {
      LOG.error("Failed to clean up intermediate multipart uploads in bucket: {}",
          info.getUfsUri(), e);
    }
  }

  /**
   * @param properties the properties for this S3 mount point
   *
   * @return the created {@link AWSCredentialsProvider} instance
   */
  private static AWSCredentialsProvider createAwsCredentialsProvider(
      Map<String, String> properties) {
    // Set the aws credential system properties based on mount point properties, if they are set;
    // otherwise, use the default credential provider.
    if (properties.containsKey(PropertyKey.S3A_ACCESS_KEY.getName())
        && properties.containsKey(PropertyKey.S3A_SECRET_KEY.getName())) {
      return new AWSStaticCredentialsProvider(new BasicAWSCredentials(
          properties.get(PropertyKey.S3A_ACCESS_KEY.getName()),
          properties.get(PropertyKey.S3A_SECRET_KEY.getName())));
    }
    // Checks, in order, env variables, system properties, profile file, and instance profile.
    return new DefaultAWSCredentialsProviderChain();
  }
}
