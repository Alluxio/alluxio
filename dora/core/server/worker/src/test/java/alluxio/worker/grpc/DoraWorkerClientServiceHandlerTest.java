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

package alluxio.worker.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.ListStatusPRequest;
import alluxio.grpc.ListStatusPResponse;
import alluxio.membership.MembershipManager;
import alluxio.worker.dora.PagedDoraWorker;

import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class DoraWorkerClientServiceHandlerTest {

  private PagedDoraWorker mWorker;
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();
  private CacheManager mCacheManager;
  private MembershipManager mMembershipManager;

  private DoraWorkerClientServiceHandler mServiceHandler;

  private ListStatusPRequest mRequest;

  @Before
  public void before() throws Exception {
    Configuration.set(PropertyKey.DORA_WORKER_METASTORE_ROCKSDB_DIR,
        mTestFolder.newFolder("rocks"));
    CacheManagerOptions cacheManagerOptions =
        CacheManagerOptions.createForWorker(Configuration.global());

    PageMetaStore pageMetaStore =
        PageMetaStore.create(CacheManagerOptions.createForWorker(Configuration.global()));
    mCacheManager =
        CacheManager.Factory.create(Configuration.global(), cacheManagerOptions, pageMetaStore);
    mMembershipManager =
        MembershipManager.Factory.create(Configuration.global());
    mWorker = new PagedDoraWorker(new AtomicReference<>(1L),
        Configuration.global(), mCacheManager, mMembershipManager);
    mServiceHandler = new DoraWorkerClientServiceHandler(mWorker);
  }

  @After
  public void after() throws Exception {
    mWorker.close();
  }

  @Test
  public void testListStatus() throws IOException {
    File rootFolder = mTestFolder.newFolder("root");
    String rootPath = rootFolder.getAbsolutePath();
    mTestFolder.newFolder("root/d1");
    mTestFolder.newFolder("root/d1/d1");
    mTestFolder.newFolder("root/d2");
    String fileContent = "test";
    File f = mTestFolder.newFile("root/f");
    Files.write(f.toPath(), fileContent.getBytes());
    mRequest = ListStatusPRequest.newBuilder().setOptions(
        alluxio.grpc.ListStatusPOptions.newBuilder().setRecursive(true).build())
        .setPath(rootPath).build();
    TestStreamObserver responseObserver = new TestStreamObserver();
    mServiceHandler.listStatus(mRequest, responseObserver);
    List<MyStruct> responses = responseObserver.mResponses;
    String[] expectedPaths = new String[] {rootPath + "/d1", rootPath + "/d1/d1",
        rootPath + "/d2", rootPath + "/f"};
    Boolean[] expectedIsDirectories = new Boolean[] {true, true, true, false};
    assertEquals(expectedPaths.length, responses.size());
    for (int i = 0; i < expectedPaths.length; i++) {
      assertEquals(expectedPaths[i], responses.get(i).getPath());
      assertEquals(expectedIsDirectories[i], responses.get(i).getIsDirectory());
      assertEquals(true, responses.get(i).getIsCompleted());
    }

    mRequest = ListStatusPRequest.newBuilder().setOptions(
        alluxio.grpc.ListStatusPOptions.newBuilder().setRecursive(false).build())
        .setPath(rootPath).build();
    responseObserver = new TestStreamObserver();
    mServiceHandler.listStatus(mRequest, responseObserver);
    responses = responseObserver.mResponses;
    expectedPaths = new String[] {rootPath + "/d1", rootPath + "/d2", rootPath + "/f"};
    expectedIsDirectories = new Boolean[] {true, true, false};
    assertEquals(expectedPaths.length, responses.size());
    for (int i = 0; i < expectedPaths.length; i++) {
      assertEquals(expectedPaths[i], responses.get(i).getPath());
      assertEquals(expectedIsDirectories[i], responses.get(i).getIsDirectory());
      assertEquals(true, responses.get(i).getIsCompleted());
    }

    mRequest = ListStatusPRequest.newBuilder().setOptions(
        alluxio.grpc.ListStatusPOptions.newBuilder().setRecursive(true).build())
        .setPath(rootPath + "/d3").build();
    responseObserver = new TestStreamObserver();
    TestStreamObserver finalResponseObserver = responseObserver;
    assertThrows(RuntimeException.class, () -> mServiceHandler.listStatus(mRequest,
        finalResponseObserver));
  }

  private static class TestStreamObserver implements StreamObserver<ListStatusPResponse> {
    private final List<MyStruct> mResponses = new ArrayList<>();

    @Override
    public void onNext(ListStatusPResponse value) {
      List<alluxio.grpc.FileInfo> fileInfosList = value.getFileInfosList();
      for (alluxio.grpc.FileInfo fileInfo : fileInfosList) {
        mResponses.add(new MyStruct(fileInfo.getPath(), fileInfo.getFolder(),
            fileInfo.getCompleted()));
      }
    }

    @Override
    public void onError(Throwable t) {
      throw new RuntimeException(t);
    }

    @Override
    public void onCompleted() {
      // do nothing
      mResponses.sort(Comparator.comparing(MyStruct::getPath));
    }
  }

  protected static class MyStruct {
    private final String mPath;
    private final Boolean mIsDirectory;
    private final Boolean mIsCompleted;

    public MyStruct(String path, Boolean isDirectory, Boolean isCompleted) {
      mPath = path;
      mIsDirectory = isDirectory;
      mIsCompleted = isCompleted;
    }

    public String getPath() {
      return mPath;
    }

    public Boolean getIsDirectory() {
      return mIsDirectory;
    }

    public Boolean getIsCompleted() {
      return mIsCompleted;
    }
  }
}
