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

package alluxio.underfs.oss;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.FormatUtils;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.junit.Assert;
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
import java.io.InputStream;
import java.security.DigestOutputStream;
import java.util.concurrent.Callable;

/**
 * Unit tests for the {@link OSSLowLevelOutputStream}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(OSSLowLevelOutputStream.class)
@SuppressWarnings("unchecked")
public class OSSLowLevelOutputStreamTest {
  private static final String BUCKET_NAME = "testBucket";
  private static final String PARTITION_SIZE = "8MB";
  private static final String KEY = "testKey";
  private static final String UPLOAD_ID = "testUploadId";
  private static InstancedConfiguration sConf = Configuration.modifiableGlobal();

  private OSS mMockOssClient;
  private ListeningExecutorService mMockExecutor;
  private BufferedOutputStream mMockOutputStream;
  private ListenableFuture<PartETag> mMockTag;

  private OSSLowLevelOutputStream mStream;

  /**
   * Sets the properties and configuration before each test runs.
   */
  @Before
  public void before() throws Exception {
    mockOSSClientAndExecutor();
    mockFileAndOutputStream();
    sConf.set(PropertyKey.UNDERFS_OSS_STREAMING_UPLOAD_PARTITION_SIZE, PARTITION_SIZE);
    mStream = new OSSLowLevelOutputStream(BUCKET_NAME, KEY, mMockOssClient, mMockExecutor, sConf);
  }

  @Test
  public void writeByte() throws Exception {
    mStream.write(1);

    mStream.close();
    Mockito.verify(mMockOutputStream).write(new byte[] {1}, 0, 1);
    Mockito.verify(mMockExecutor, never()).submit(any(Callable.class));
    Mockito.verify(mMockOssClient).putObject(any(PutObjectRequest.class));
    Mockito.verify(mMockOssClient, never())
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    Mockito.verify(mMockOssClient, never())
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  public void writeByteArrayForSmallFile() throws Exception {
    int partSize = (int) FormatUtils.parseSpaceSize(PARTITION_SIZE);
    byte[] b = new byte[partSize];

    mStream.write(b, 0, b.length);
    Mockito.verify(mMockOutputStream).write(b, 0, b.length);

    mStream.close();
    Mockito.verify(mMockExecutor, never()).submit(any(Callable.class));
    Mockito.verify(mMockOssClient).putObject(any(PutObjectRequest.class));
    Mockito.verify(mMockOssClient, never())
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    Mockito.verify(mMockOssClient, never())
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  public void writeByteArrayForLargeFile() throws Exception {
    int partSize = (int) FormatUtils.parseSpaceSize(PARTITION_SIZE);
    byte[] b = new byte[partSize + 1];
    Assert.assertEquals(mStream.getPartNumber(), 1);
    mStream.write(b, 0, b.length);
    Assert.assertEquals(mStream.getPartNumber(), 2);
    Mockito.verify(mMockOssClient)
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    Mockito.verify(mMockOutputStream).write(b, 0, b.length - 1);
    Mockito.verify(mMockOutputStream).write(b, b.length - 1, 1);
    Mockito.verify(mMockExecutor).submit(any(Callable.class));

    mStream.close();
    Assert.assertEquals(mStream.getPartNumber(), 3);
    Mockito.verify(mMockOssClient)
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  public void createEmptyFile() throws Exception {
    mStream.close();
    Mockito.verify(mMockExecutor, never()).submit(any(Callable.class));
    Mockito.verify(mMockOssClient, never())
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    Mockito.verify(mMockOssClient, never())
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    Mockito.verify(mMockOssClient).putObject(eq(BUCKET_NAME), eq(KEY), any(InputStream.class),
        any(ObjectMetadata.class));
  }

  @Test
  public void flush() throws Exception {
    int partSize = (int) FormatUtils.parseSpaceSize(PARTITION_SIZE);
    byte[] b = new byte[2 * partSize - 1];

    mStream.write(b, 0, b.length);
    Mockito.verify(mMockOssClient)
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    Mockito.verify(mMockOutputStream).write(b, 0, partSize);
    Mockito.verify(mMockOutputStream).write(b, partSize, partSize - 1);
    Mockito.verify(mMockExecutor).submit(any(Callable.class));

    mStream.flush();
    Mockito.verify(mMockExecutor, times(2)).submit(any(Callable.class));
    Mockito.verify(mMockTag, times(2)).get();

    mStream.close();
    Mockito.verify(mMockOssClient)
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  public void close() throws Exception {
    mStream.close();
    Mockito.verify(mMockOssClient, never())
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    Mockito.verify(mMockOssClient, never())
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  /**
   * Mocks the OSS client and executor.
   */
  private void mockOSSClientAndExecutor() throws Exception {
    mMockOssClient = PowerMockito.mock(OSS.class);

    InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
    when(mMockOssClient.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
        .thenReturn(initResult);

    initResult.setUploadId(UPLOAD_ID);
    when(mMockOssClient.uploadPart(any(UploadPartRequest.class)))
        .thenAnswer((InvocationOnMock invocation) -> {
          Object[] args = invocation.getArguments();
          UploadPartResult uploadResult = new UploadPartResult();
          uploadResult.setPartNumber(((UploadPartRequest) args[0]).getPartNumber());
          return uploadResult;
        });

    when(mMockOssClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
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
