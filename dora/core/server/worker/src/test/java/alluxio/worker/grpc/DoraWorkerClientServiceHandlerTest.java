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

import alluxio.client.file.FileSystemContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.ListStatusPRequest;
import alluxio.grpc.ListStatusPResponse;
import alluxio.membership.MembershipManager;
import alluxio.util.io.PathUtils;
import alluxio.wire.WorkerIdentity;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.dora.DoraMetaManager;
import alluxio.worker.dora.DoraUfsManager;
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
    DoraUfsManager ufsManager = new DoraUfsManager();
    DoraMetaManager metaManager = new DoraMetaManager(Configuration.global(),
        mCacheManager, ufsManager);
    mWorker = new PagedDoraWorker(new AtomicReference<>(
            WorkerIdentity.ParserV0.INSTANCE.fromLong(1L)),
            Configuration.global(), mCacheManager, mMembershipManager, new BlockMasterClientPool(),
            ufsManager, metaManager, FileSystemContext.create());
    mServiceHandler = new DoraWorkerClientServiceHandler(mWorker);
  }

  @After
  public void after() throws Exception {
    mWorker.close();
  }

  @Test
  public void testRecursiveListStatus() throws IOException {
    // Setup
    String rootPath = setupTestDirectoryAndFile();

    // Execute and Assert
    executeAndAssertListStatus(rootPath, true, new String[] {
        PathUtils.concatPath(rootPath, "d1"),
        PathUtils.concatPath(rootPath, "d1/d1"),
        PathUtils.concatPath(rootPath, "d2"),
        PathUtils.concatPath(rootPath, "f")
    }, new Boolean[] {true, true, true, false});
  }

  @Test
  public void testNonRecursiveListStatus() throws IOException {
    // Setup
    String rootPath = setupTestDirectoryAndFile();

    // Execute and Assert
    executeAndAssertListStatus(rootPath, false, new String[] {
        PathUtils.concatPath(rootPath, "d1"),
        PathUtils.concatPath(rootPath, "d2"),
        PathUtils.concatPath(rootPath, "f")
    }, new Boolean[] {true, true, false});
  }

  @Test
  public void testListStatusNonExistentDirectory() throws IOException {
    // Setup
    String rootPath = setupTestDirectoryAndFile();

    // Test and Assert
    String nonExistentPath = PathUtils.concatPath(rootPath, "d3");
    TestStreamObserver responseObserver = new TestStreamObserver();
    mRequest = ListStatusPRequest.newBuilder().setOptions(
            alluxio.grpc.ListStatusPOptions.newBuilder().setRecursive(true).build())
        .setPath(nonExistentPath).build();
    assertThrows(RuntimeException.class, () -> mServiceHandler.listStatus(mRequest,
        responseObserver));
  }

  @Test
  public void testListStatusOnFile() throws IOException {
    // Setup
    String rootPath = setupTestDirectoryAndFile();
    String filePath = PathUtils.concatPath(rootPath, "f");

    // Execute and Assert
    executeAndAssertListStatus(filePath, true, new String[] {filePath}, new Boolean[] {false});
  }

  private String setupTestDirectoryAndFile() throws IOException {
    File rootFolder = mTestFolder.newFolder("root");
    String rootPath = rootFolder.getAbsolutePath();
    mTestFolder.newFolder("root/d1");
    mTestFolder.newFolder("root/d1/d1");
    mTestFolder.newFolder("root/d2");
    String fileContent = "test";
    File f = mTestFolder.newFile("root/f");
    Files.write(f.toPath(), fileContent.getBytes());

    return rootPath;
  }

  private void executeAndAssertListStatus(String path, boolean recursive, String[] expectedPaths,
                                          Boolean[] expectedIsDirectories) {
    mRequest = ListStatusPRequest.newBuilder().setOptions(
            alluxio.grpc.ListStatusPOptions.newBuilder().setRecursive(recursive).build())
        .setPath(path).build();
    TestStreamObserver responseObserver = new TestStreamObserver();
    mServiceHandler.listStatus(mRequest, responseObserver);

    List<MyStruct> responses = responseObserver.mResponses;
    assertEquals(expectedPaths.length, responses.size());
    for (int i = 0; i < expectedPaths.length; i++) {
      assertEquals(expectedPaths[i], responses.get(i).getPath());
      assertEquals(expectedIsDirectories[i], responses.get(i).getIsDirectory());
      assertEquals(true, responses.get(i).getIsCompleted());
    }
  }

  private static class TestStreamObserver implements StreamObserver<ListStatusPResponse> {
    private final List<MyStruct> mResponses = new ArrayList<>();

    @Override
    public void onNext(ListStatusPResponse value) {
      List<alluxio.grpc.FileInfo> fileInfosList = value.getFileInfosList();
      for (alluxio.grpc.FileInfo fileInfo : fileInfosList) {
        System.out.println(fileInfo);
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
