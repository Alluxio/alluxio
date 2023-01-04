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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.FormatUtils;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;
import com.google.common.util.concurrent.ListenableFuture;
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
import java.nio.file.Files;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ListenableFuture.class, Executors.class, OSSClient.class,
    MultipartOSSOutputStreamTest.class, BufferedOutputStream.class,
    MultipartOSSOutputStream.class, Files.class})
public class MultipartOSSOutputStreamTest {
  private static final String BUCKET_NAME = "testBucket";
  private static final String PARTITION_SIZE = "8MB";
  private static final String KEY = "testKey";
  private static final String UPLOAD_ID = "testUploadId";
  private static InstancedConfiguration sConf = Configuration.copyGlobal();

  private OSSClient mOssClient;
  private BufferedOutputStream mMockOutputStream;
  private ListenableFuture<PartETag> mMockTag;
  private ExecutorService mMockExecutor;
  private MultipartOSSOutputStream mStream;

  @Before
  public void before() throws Exception {
    mockOSSClient();
    mockFileAndOutputStream();
    sConf.set(PropertyKey.UNDERFS_OSS_MULTIPART_UPLOAD_PARTITION_SIZE, PARTITION_SIZE);
    mStream = new MultipartOSSOutputStream(BUCKET_NAME, KEY, mOssClient,
        sConf.getList(PropertyKey.TMP_DIRS),
        sConf.getBytes(PropertyKey.UNDERFS_OSS_MULTIPART_UPLOAD_PARTITION_SIZE),
        sConf.getInt(PropertyKey.UNDERFS_OSS_MULTIPART_UPLOAD_POOL_SIZE));
  }

  @Test
  public void testWriteByte() throws Exception {
    mStream.write(1);
    Mockito.verify(mOssClient)
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    Mockito.verify(mMockOutputStream).write(new byte[]{1}, 0, 1);
    Mockito.verify(mMockExecutor, never()).submit(any(Callable.class));

    mStream.close();
    Mockito.verify(mMockExecutor).submit(any(Callable.class));
    Mockito.verify(mOssClient)
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  public void testWriteByteArray() throws Exception {
    int partSize = (int) FormatUtils.parseSpaceSize(PARTITION_SIZE);
    byte[] b = new byte[partSize + 1];

    mStream.write(b, 0, b.length);
    Mockito.verify(mOssClient)
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    Mockito.verify(mMockOutputStream).write(b, 0, b.length - 1);
    Mockito.verify(mMockOutputStream).write(b, b.length - 1, 1);
    Mockito.verify(mMockExecutor).submit(any(Callable.class));

    mStream.close();
    Mockito.verify(mOssClient)
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  public void testFlush() throws Exception {
    int partSize = (int) FormatUtils.parseSpaceSize(PARTITION_SIZE);
    byte[] b = new byte[2 * partSize - 1];

    mStream.write(b, 0, b.length);
    Mockito.verify(mOssClient)
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    Mockito.verify(mMockOutputStream).write(b, 0, partSize);
    Mockito.verify(mMockOutputStream).write(b, partSize, partSize - 1);
    Mockito.verify(mMockExecutor).submit(any(Callable.class));

    mStream.flush();
    Mockito.verify(mMockExecutor, times(1)).submit(any(Callable.class));

    mStream.close();
    Mockito.verify(mOssClient)
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  @Test
  public void testClose() throws Exception {
    mStream.close();
    Mockito.verify(mOssClient, never())
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));
    Mockito.verify(mOssClient, never())
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  private void mockOSSClient() throws ExecutionException, InterruptedException {
    mOssClient = Mockito.mock(OSSClient.class);
    InitiateMultipartUploadResult initResult = new InitiateMultipartUploadResult();
    when(mOssClient.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
        .thenReturn(initResult);
    initResult.setUploadId(UPLOAD_ID);
    when(mOssClient.uploadPart(any(UploadPartRequest.class)))
        .thenAnswer((InvocationOnMock invocation) -> {
          Object[] args = invocation.getArguments();
          UploadPartResult uploadResult = new UploadPartResult();
          uploadResult.setPartNumber(((UploadPartRequest) args[0]).getPartNumber());
          return uploadResult;
        });

    when(mOssClient.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
        .thenReturn(new CompleteMultipartUploadResult());

    mMockTag = (ListenableFuture<PartETag>) PowerMockito.mock(ListenableFuture.class);
    when(mMockTag.get()).thenReturn(new PartETag(1, "someTag"));
    mMockExecutor = Mockito.mock(ThreadPoolExecutor.class);
    PowerMockito.mockStatic(Executors.class);
    when(Executors.newFixedThreadPool(anyInt())).thenReturn(mMockExecutor);
    when(mMockExecutor.submit(any(Callable.class))).thenReturn(mMockTag);
  }

  private void mockFileAndOutputStream() throws Exception {
    File file = Mockito.mock(File.class);
    PowerMockito.whenNew(File.class).withAnyArguments().thenReturn(file);

    mMockOutputStream = Mockito.mock(BufferedOutputStream.class);
    PowerMockito.whenNew(BufferedOutputStream.class)
        .withArguments(Mockito.any(FileOutputStream.class)).thenReturn(mMockOutputStream);

    FileOutputStream outputStream = PowerMockito.mock(FileOutputStream.class);
    PowerMockito.whenNew(FileOutputStream.class).withArguments(file).thenReturn(outputStream);
    PowerMockito.mockStatic(Files.class);
    PowerMockito.when(Files.newOutputStream(any())).thenReturn(outputStream);
  }
}
