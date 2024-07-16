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

package alluxio.underfs.tos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.volcengine.tos.TOSV2;
import com.volcengine.tos.model.object.CompleteMultipartUploadV2Input;
import com.volcengine.tos.model.object.CompleteMultipartUploadV2Output;
import com.volcengine.tos.model.object.CreateMultipartUploadInput;
import com.volcengine.tos.model.object.CreateMultipartUploadOutput;
import com.volcengine.tos.model.object.PutObjectInput;
import com.volcengine.tos.model.object.PutObjectOutput;
import com.volcengine.tos.model.object.UploadPartV2Input;
import com.volcengine.tos.model.object.UploadPartV2Output;
import com.volcengine.tos.model.object.UploadedPartV2;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.Callable;

/**
 * Unit tests for the {@link TOSLowLevelOutputStream}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TOSLowLevelOutputStream.class)
public class TOSLowLevelOutputStreamTest {
  private static final String BUCKET_NAME = "testBucket";
  private static final String PARTITION_SIZE = "8MB";
  private static final String KEY = "testKey";
  private static final String UPLOAD_ID = "testUploadId";
  private InstancedConfiguration mConf = Configuration.copyGlobal();

  private TOSV2 mMockTosClient;
  private ListeningExecutorService mMockExecutor;
  private ListenableFuture<UploadedPartV2> mMockTag;
  private TOSLowLevelOutputStream mStream;

  @Before
  public void before() throws Exception {
    mockTOSClientAndExecutor();
    mConf.set(PropertyKey.UNDERFS_TOS_STREAMING_UPLOAD_PARTITION_SIZE, PARTITION_SIZE);
    mConf.set(PropertyKey.UNDERFS_TOS_STREAMING_UPLOAD_ENABLED, "true");
    mStream = new TOSLowLevelOutputStream(BUCKET_NAME, KEY, mMockTosClient, mMockExecutor, mConf);
  }

  @Test
  public void writeByte() throws Exception {
    mStream.write(1);

    mStream.close();
    Mockito.verify(mMockExecutor, never()).submit(any(Callable.class));
    Mockito.verify(mMockTosClient).putObject(any(PutObjectInput.class));
    Mockito.verify(mMockTosClient, never())
        .createMultipartUpload(any(CreateMultipartUploadInput.class));
    Mockito.verify(mMockTosClient, never()).completeMultipartUpload(any(
        CompleteMultipartUploadV2Input.class));
    assertTrue(mStream.getContentHash().isPresent());
    assertEquals("putTag", mStream.getContentHash().get());
  }

  @Test
  public void writeByteArrayForSmallFile() throws Exception {
    int partSize = (int) (8 * 1024 * 1024); // 8MB
    byte[] b = new byte[partSize];

    mStream.write(b, 0, b.length);

    mStream.close();
    Mockito.verify(mMockExecutor, never()).submit(any(Callable.class));
    Mockito.verify(mMockTosClient).putObject(any(PutObjectInput.class));
    Mockito.verify(mMockTosClient, never())
        .createMultipartUpload(any(CreateMultipartUploadInput.class));
    Mockito.verify(mMockTosClient, never())
        .completeMultipartUpload(any(CompleteMultipartUploadV2Input.class));
    assertTrue(mStream.getContentHash().isPresent());
    assertEquals("putTag", mStream.getContentHash().get());
  }

  @Test
  public void writeByteArrayForLargeFile() throws Exception {
    int partSize = (int) (8 * 1024 * 1024); // 8MB
    byte[] b = new byte[partSize + 1];

    mStream.write(b, 0, b.length);

    mStream.close();
    Mockito.verify(mMockTosClient).createMultipartUpload(any(CreateMultipartUploadInput.class));
    Mockito.verify(mMockExecutor, times(2)).submit(any(Callable.class));
    Mockito.verify(mMockTosClient)
        .completeMultipartUpload(any(CompleteMultipartUploadV2Input.class));
    assertTrue(mStream.getContentHash().isPresent());
    assertEquals("multiTag", mStream.getContentHash().get());
  }

  @Test
  public void createEmptyFile() throws Exception {
    mStream.close();
    Mockito.verify(mMockExecutor, never()).submit(any(Callable.class));
    Mockito.verify(mMockTosClient, never())
        .createMultipartUpload(any(CreateMultipartUploadInput.class));
    Mockito.verify(mMockTosClient, never())
        .completeMultipartUpload(any(CompleteMultipartUploadV2Input.class));
    Mockito.verify(mMockTosClient).putObject(any(PutObjectInput.class));
    assertTrue(mStream.getContentHash().isPresent());
    assertEquals("emptyTag", mStream.getContentHash().get());
  }

  @Test
  public void flush() throws Exception {
    int partSize = (int) (8 * 1024 * 1024); // 8MB
    byte[] b = new byte[2 * partSize - 1];

    mStream.write(b, 0, b.length);

    mStream.flush();
    Mockito.verify(mMockExecutor, times(2)).submit(any(Callable.class));
    Mockito.verify(mMockTag, times(2)).get();

    mStream.close();
    Mockito.verify(mMockTosClient)
        .completeMultipartUpload(any(CompleteMultipartUploadV2Input.class));
    assertTrue(mStream.getContentHash().isPresent());
    assertEquals("multiTag", mStream.getContentHash().get());
  }

  @Test
  public void close() throws Exception {
    mStream.close();
    Mockito.verify(mMockTosClient, never())
        .createMultipartUpload(any(CreateMultipartUploadInput.class));
    Mockito.verify(mMockTosClient, never())
        .completeMultipartUpload(any(CompleteMultipartUploadV2Input.class));
    assertTrue(mStream.getContentHash().isPresent());
    assertEquals("emptyTag", mStream.getContentHash().get());
  }

  private void mockTOSClientAndExecutor() throws Exception {
    mMockTosClient = PowerMockito.mock(TOSV2.class);

    CreateMultipartUploadOutput createOutput = new CreateMultipartUploadOutput();
    createOutput.setUploadID(UPLOAD_ID);
    when(mMockTosClient.createMultipartUpload(any(CreateMultipartUploadInput.class)))
        .thenReturn(createOutput);

    UploadPartV2Output uploadPartOutput = new UploadPartV2Output();
    uploadPartOutput.setEtag("partTag");
    when(mMockTosClient.uploadPart(any(UploadPartV2Input.class))).thenReturn(uploadPartOutput);

    // Use Answer to dynamically return PutObjectOutput based on the input
    when(mMockTosClient.putObject(any(PutObjectInput.class)))
        .thenAnswer(new Answer<PutObjectOutput>() {
          @Override
          public PutObjectOutput answer(InvocationOnMock invocation) throws Throwable {
            PutObjectInput input = invocation.getArgument(0);
            PutObjectOutput output = new PutObjectOutput();
            // Determine the Etag value based on the input condition
            if (input.getContentLength() == 0) {
              output.setEtag("emptyTag");
            } else {
              output.setEtag("putTag");
            }
            return output;
          }
        });

    CompleteMultipartUploadV2Output completeOutput = new CompleteMultipartUploadV2Output();
    completeOutput.setEtag("multiTag");
    when(mMockTosClient.completeMultipartUpload(any(CompleteMultipartUploadV2Input.class)))
        .thenReturn(completeOutput);

    mMockTag = (ListenableFuture<UploadedPartV2>) PowerMockito.mock(ListenableFuture.class);
    when(mMockTag.get()).thenReturn(new UploadedPartV2().setPartNumber(1).setEtag("partTag"));
    mMockExecutor = Mockito.mock(ListeningExecutorService.class);
    when(mMockExecutor.submit(any(Callable.class))).thenReturn(mMockTag);
  }
}
