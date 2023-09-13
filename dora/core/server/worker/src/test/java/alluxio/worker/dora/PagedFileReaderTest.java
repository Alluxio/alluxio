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

package alluxio.worker.dora;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.PositionReaderTest;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.DefaultPageMetaStore;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.file.cache.evictor.CacheEvictorOptions;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.store.MemoryPageStore;
import alluxio.client.file.cache.store.MemoryPageStoreDir;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.network.protocol.databuffer.CompositeDataBuffer;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.io.BufferUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Unit test of {@link PagedFileReader}.
 */
@RunWith(Parameterized.class)
public class PagedFileReaderTest {

  @Parameterized.Parameter
  public int mFileLen;
  @Rule
  public TemporaryFolder mTemporaryFolder = new TemporaryFolder();
  private final InstancedConfiguration mConf = Configuration.copyGlobal();
  private String mTestFileName;
  private PagedFileReader mPagedFileReader;
  private UnderFileSystem mLocalUfs;
  private byte[] mTestData;
  private final int mMinTestNum = 50;
  private final Random mRandom = new Random();
  private final EmbeddedChannel mEmbeddedChannel = new EmbeddedChannel();

  private PositionReaderTest mPositionReaderTest;

  @Parameterized.Parameters(name = "{index}-{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {0},
        {1},
        {128},
        {256},
        {666},
        {5314},
        {Constants.KB - 1},
        {Constants.KB},
        {Constants.KB + 1},
        {64 * Constants.KB - 1},
        {64 * Constants.KB},
        {64 * Constants.KB + 1},
    });
  }

  @Before
  public void before() throws IOException {
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PageStoreType.MEM);
    PageStoreOptions pageStoreOptions = PageStoreOptions.create(mConf).get(0);
    CacheEvictor evictor = new FIFOCacheEvictor(new CacheEvictorOptions());
    PageStoreDir pageStoreDir = new MemoryPageStoreDir(pageStoreOptions,
        (MemoryPageStore) PageStore.create(pageStoreOptions), evictor);
    PageMetaStore pageMetaStore = new DefaultPageMetaStore(ImmutableList.of(pageStoreDir));

    String localUfsRoot = mTemporaryFolder.getRoot().getAbsolutePath();
    mLocalUfs = UnderFileSystem.Factory.create(
        localUfsRoot, UnderFileSystemConfiguration.defaults(mConf));
    Path path = Paths.get(localUfsRoot, "testFile" + UUID.randomUUID());
    mTestData = BufferUtils.getIncreasingByteArray(mFileLen);
    try (FileOutputStream os = new FileOutputStream(path.toFile())) {
      os.write(mTestData);
    }
    mTestFileName = path.toString();
    CacheManagerOptions cacheManagerOptions = CacheManagerOptions
        .createForWorker(mConf);
    String fileId = new AlluxioURI(mTestFileName).hash();
    CacheManager cacheManager = CacheManager.Factory.create(
        Configuration.global(), cacheManagerOptions, pageMetaStore);
    mPagedFileReader = PagedFileReader.create(
        mConf, cacheManager, mLocalUfs, fileId, mTestFileName, mFileLen, 0);
    mPositionReaderTest = new PositionReaderTest(mPagedFileReader, mFileLen);
  }

  @After
  public void after() throws IOException {
    mPagedFileReader.close();
    new File(mTestFileName).delete();
    mLocalUfs.close();
  }

  @Test
  public void testAllCornerCases() throws IOException {
    mPositionReaderTest.testAllCornerCases();
  }

  @Test
  public void testReadRandomPart() throws IOException {
    mPositionReaderTest.testReadRandomPart();
  }

  @Test
  public void testConcurrentReadRandomPart() throws Exception {
    mPositionReaderTest.concurrentReadPart();
  }

  @Test
  public void read() throws IOException {
    if (mFileLen > 0) {
      mPagedFileReader.setmPos(mFileLen);
      int testNum = Math.min(mFileLen, mMinTestNum);
      for (int i = 0; i < testNum; i++) {
        int offset = mRandom.nextInt(mFileLen);
        int readLength = mRandom.nextInt(mFileLen - offset);
        ByteBuffer buf = ByteBuffer.allocate(mFileLen);
        byte[] tmpBytes = Files.readAllBytes(Paths.get(mTestFileName));
        byte[] bytes = Arrays.copyOfRange(tmpBytes, offset, offset + readLength);
        ByteBuffer realByteBuffer = ByteBuffer.wrap(bytes);
        ByteBuffer byteBuffer = mPagedFileReader.read(offset, readLength);
        Assert.assertEquals(realByteBuffer, byteBuffer);
      }
    }
  }

  @Test
  public void transferTo() throws IOException {
    int testNum = Math.min(mFileLen, mMinTestNum);
    for (int i = 0; i < testNum; i++) {
      int offset = mRandom.nextInt(mFileLen);
      mPagedFileReader.setmPos(offset);
      ByteBuf byteBuf = Unpooled.buffer(mFileLen);

      int bytesRead = mPagedFileReader.transferTo(byteBuf);

      byte[] bytesArray = new byte[byteBuf.readableBytes()];
      byteBuf.readBytes(bytesArray);
      List<Byte> listOfByte = Bytes.asList(bytesArray);
      List<Byte> listOfRealByte = Bytes.asList(mTestData).subList(offset, offset + bytesRead);
      assertTrue((mFileLen == 0 && bytesRead == -1) || bytesRead > 0);
      assertEquals(listOfRealByte, listOfByte);
    }
  }

  @Test
  public void getLength() {
    Assert.assertEquals(mPagedFileReader.getLength(), mFileLen);
  }

  @Test
  public void getMultipleDataFileChannel() throws IOException {
    if (mFileLen > 0) {
      CompositeDataBuffer compositeDataBuffer =
          mPagedFileReader.getMultipleDataFileChannel(mEmbeddedChannel, mFileLen);
      // TODO(Tony Sun): How to verify the correctness of CompositeDataBuffer?
      Assert.assertNotNull(compositeDataBuffer);
    } else {
      Assert.assertNull(mPagedFileReader.getMultipleDataFileChannel(mEmbeddedChannel, mFileLen));
    }
  }
}
