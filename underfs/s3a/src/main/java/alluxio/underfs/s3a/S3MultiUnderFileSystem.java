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
import alluxio.conf.PropertyKey;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.util.concurrent.ExecutorService;

/**
 * For mounting the same bucket multiple times.
 */
public class S3MultiUnderFileSystem extends S3AUnderFileSystem {

  private final String mRootKey;

  /**
   * Constructor for {@link S3AUnderFileSystem}.
   *
   * @param uri                    the {@link AlluxioURI} for this UFS
   * @param amazonS3Client         AWS-SDK S3 client
   * @param asyncClient
   * @param bucketName             bucket name of user's configured Alluxio bucket
   * @param executor               the executor for executing upload tasks
   * @param transferManager        Transfer Manager for efficient I/O to S3
   * @param conf                   configuration for this S3A ufs
   * @param streamingUploadEnabled whether streaming upload is enabled
   */
  protected S3MultiUnderFileSystem(
      AlluxioURI uri, AmazonS3 amazonS3Client, S3AsyncClient asyncClient, String bucketName,
      ExecutorService executor, TransferManager transferManager,
      UnderFileSystemConfiguration conf, boolean streamingUploadEnabled) {
    super(uri, amazonS3Client, asyncClient, bucketName, executor, transferManager,
        conf, streamingUploadEnabled);

    mRootKey = mUri.getScheme() + "://" + bucketName;
  }

  @Override
  protected String getRootKey() {
    return mRootKey;
  }

  /**
   * Constructs a new instance of {@link S3AUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return the created {@link S3AUnderFileSystem} instance
   */
  public static S3AUnderFileSystem createInstance(AlluxioURI uri,
                                                  UnderFileSystemConfiguration conf) {

    AWSCredentialsProvider credentials = createAwsCredentialsProvider(conf);
    String bucketName = UnderFileSystemUtils.getBucketName(uri);

    // Set the client configuration based on Alluxio configuration values.
    ClientConfiguration clientConf = new ClientConfiguration();

    // Max error retry
    if (conf.isSet(PropertyKey.UNDERFS_S3_MAX_ERROR_RETRY)) {
      clientConf.setMaxErrorRetry(conf.getInt(PropertyKey.UNDERFS_S3_MAX_ERROR_RETRY));
    }
    clientConf.setConnectionTTL(conf.getMs(PropertyKey.UNDERFS_S3_CONNECT_TTL));
    // Socket timeout
    clientConf
        .setSocketTimeout((int) conf.getMs(PropertyKey.UNDERFS_S3_SOCKET_TIMEOUT));

    // HTTP protocol
    if (conf.getBoolean(PropertyKey.UNDERFS_S3_SECURE_HTTP_ENABLED)
        || conf.getBoolean(PropertyKey.UNDERFS_S3_SERVER_SIDE_ENCRYPTION_ENABLED)) {
      clientConf.setProtocol(Protocol.HTTPS);
    } else {
      clientConf.setProtocol(Protocol.HTTP);
    }

    // Proxy host
    if (conf.isSet(PropertyKey.UNDERFS_S3_PROXY_HOST)) {
      clientConf.setProxyHost(conf.getString(PropertyKey.UNDERFS_S3_PROXY_HOST));
    }

    // Proxy port
    if (conf.isSet(PropertyKey.UNDERFS_S3_PROXY_PORT)) {
      clientConf.setProxyPort(conf.getInt(PropertyKey.UNDERFS_S3_PROXY_PORT));
    }

    // Number of metadata and I/O threads to S3.
    int numAdminThreads = conf.getInt(PropertyKey.UNDERFS_S3_ADMIN_THREADS_MAX);
    int numTransferThreads =
        conf.getInt(PropertyKey.UNDERFS_S3_UPLOAD_THREADS_MAX);
    int numThreads = conf.getInt(PropertyKey.UNDERFS_S3_THREADS_MAX);
    if (numThreads < numAdminThreads + numTransferThreads) {
      numThreads = numAdminThreads + numTransferThreads;
    }
    clientConf.setMaxConnections(numThreads);

    // Set client request timeout for all requests since multipart copy is used,
    // and copy parts can only be set with the client configuration.
    clientConf
        .setRequestTimeout((int) conf.getMs(PropertyKey.UNDERFS_S3_REQUEST_TIMEOUT));

    boolean streamingUploadEnabled =
        conf.getBoolean(PropertyKey.UNDERFS_S3_STREAMING_UPLOAD_ENABLED);

    // Signer algorithm
    if (conf.isSet(PropertyKey.UNDERFS_S3_SIGNER_ALGORITHM)) {
      clientConf.setSignerOverride(conf.getString(PropertyKey.UNDERFS_S3_SIGNER_ALGORITHM));
    }

    AwsClientBuilder.EndpointConfiguration endpointConfiguration
        = createEndpointConfiguration(conf, clientConf);

    AmazonS3 amazonS3Client
        = createAmazonS3(credentials, clientConf, endpointConfiguration, conf);
    S3AsyncClient asyncClient = createAmazonS3Async(conf, clientConf);

    ExecutorService service = ExecutorServiceFactories
        .fixedThreadPool("alluxio-s3-transfer-manager-worker",
            numTransferThreads).create();

    TransferManager transferManager = TransferManagerBuilder.standard()
        .withS3Client(amazonS3Client)
        .withExecutorFactory(() -> service)
        .withMultipartCopyThreshold(MULTIPART_COPY_THRESHOLD)
        .build();

    return new S3MultiUnderFileSystem(uri, amazonS3Client, asyncClient, bucketName,
        service, transferManager, conf, streamingUploadEnabled);
  }
}
