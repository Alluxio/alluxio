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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.util.FormatUtils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.security.DigestOutputStream;
import java.util.concurrent.Callable;

/**
 * Unit tests for the {@link S3ALowLevelOutputStream}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(S3ALowLevelOutputStream.class)
@SuppressWarnings("unchecked")
public class S3ALowLevelOutputStreamTest {
  private static final String BUCKET_NAME = "testBucket";
  private static final String PARTITION_SIZE = "8MB";
  private static final String KEY = "testKey";
  private static final String UPLOAD_ID = "testUploadId";
  private static InstancedConfiguration sConf = new InstancedConfiguration(
      ConfigurationUtils.defaults());

  private AmazonS3 mMockS3Client;
  private ListeningExecutorService mMockExecutor;
  private BufferedOutputStream mMockOutputStream;
  private ListenableFuture<PartETag> mMockTag;

  private S3ALowLevelOutputStream mStream;

  /**
   * Sets the properties and configuration before each test runs.
   */
  @Before
  public void before() throws Exception {
    mockS3ClientAndExecutor();
    mockFileAndOutputStream();
    sConf.set(PropertyKey.UNDERFS_S3_STREAMING_UPLOAD_PARTITION_SIZE, PARTITION_SIZE);
    mStream = new S3ALowLevelOutputStream(BUCKET_NAME, KEY, mMockS3Client, mMockExecutor,
        sConf.getBytes(PropertyKey.UNDERFS_S3_STREAMING_UPLOAD_PARTITION_SIZE),
        sConf.getList(PropertyKey.TMP_DIRS, ","),
        sConf.getBoolean(PropertyKey.UNDERFS_S3_SERVER_SIDE_ENCRYPTION_ENABLED));
  }

  @Test
  public void writeByte() throws Exception {
    mStream.write(1);
    Mockito.verify(mMockS3Client)
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    Mockito.verify(mMockOutputStream).write(new byte[]{1}, 0, 1);
    Mockito.verify(mMockExecutor, never()).submit(any(Callable.class));

    mStream.close();
    Mockito.verify(mMockExecutor).submit(any(Callable.class));
    Mockito.verify(mMockS3Client)
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  public void writeByteArray() throws Exception {
    int partSize = (int) FormatUtils.parseSpaceSize(PARTITION_SIZE);
    byte[] b = new byte[partSize + 1];

    mStream.write(b, 0, b.length);
    Mockito.verify(mMockS3Client)
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    Mockito.verify(mMockOutputStream).write(b, 0, b.length - 1);
    Mockito.verify(mMockOutputStream).write(b, b.length - 1, 1);
    Mockito.verify(mMockExecutor).submit(any(Callable.class));

    mStream.close();
    Mockito.verify(mMockS3Client)
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  public void flush() throws Exception {
    int partSize = (int) FormatUtils.parseSpaceSize(PARTITION_SIZE);
    byte[] b = new byte[2 * partSize - 1];

    mStream.write(b, 0, b.length);
    Mockito.verify(mMockS3Client)
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    Mockito.verify(mMockOutputStream).write(b, 0, partSize);
    Mockito.verify(mMockOutputStream).write(b, partSize, partSize - 1);
    Mockito.verify(mMockExecutor).submit(any(Callable.class));

    mStream.flush();
    Mockito.verify(mMockExecutor, times(2)).submit(any(Callable.class));
    Mockito.verify(mMockTag, times(2)).get();

    mStream.close();
    Mockito.verify(mMockS3Client)
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  public void close() throws Exception {
    mStream.close();
    Mockito.verify(mMockS3Client, never())
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    Mockito.verify(mMockS3Client, never())
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  /**
   * Mocks the S3 client and executor.
   */
  private void mockS3ClientAndExecutor() throws Exception {
    mMockS3Client = PowerMockito.mock(AmazonS3.class);

    InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
    when(mMockS3Client.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
        .thenReturn(initResult);

    initResult.setUploadId(UPLOAD_ID);
    when(mMockS3Client.uploadPart(any(UploadPartRequest.class)))
        .thenAnswer((InvocationOnMock invocation) -> {
          Object[] args = invocation.getArguments();
          UploadPartResult uploadResult = new UploadPartResult();
          uploadResult.setPartNumber(((UploadPartRequest) args[0]).getPartNumber());
          return uploadResult;
        });

    when(mMockS3Client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
        .thenReturn(new CompleteMultipartUploadResult());

    mMockTag = (ListenableFuture<PartETag>) PowerMockito.mock(ListenableFuture.class);
    when(mMockTag.get()).thenReturn(new PartETag(1, "someTag"));
    mMockExecutor = Mockito.mock(ListeningExecutorService.class);
    when(mMockExecutor.submit(any(Callable.class))).thenReturn(mMockTag);
  }

  /**
   * Mocks file-related classes.
   */
  private void mockFileAndOutputStream() throws Exception {
    File file = Mockito.mock(File.class);
    PowerMockito.whenNew(File.class).withAnyArguments().thenReturn(file);

    mMockOutputStream = PowerMockito.mock(BufferedOutputStream.class);
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(Mockito.any(DigestOutputStream.class)).thenReturn(mMockOutputStream);

    FileOutputStream outputStream = PowerMockito.mock(FileOutputStream.class);
    PowerMockito.whenNew(FileOutputStream.class).withArguments(file).thenReturn(outputStream);
  }
}
