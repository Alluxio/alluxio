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

import alluxio.grpc.FileInfo;
import alluxio.proto.meta.DoraMeta;

import junit.framework.TestCase;

import java.util.Optional;

public class RocksDBDoraMetaStoreTest extends TestCase {
  private RocksDBDoraMetaStore mTestMetastore;

  public void setUp() throws Exception {
    super.setUp();
    mTestMetastore = new RocksDBDoraMetaStore("/tmp/");
    System.out.println("Setup");
  }

  public void tearDown() throws Exception {
    mTestMetastore.close();
    System.out.println("tearDown");
  }

  public void testPutAndGetAndDel() {
    System.out.println("Start testPutAndGet");

    String path = new String("/HELLO");
    FileInfo fi = FileInfo.newBuilder()
        .setFileId(1234)
        .setMode(0567)
        .setLength(1000)
        .build();
    DoraMeta.FileStatus fs = DoraMeta.FileStatus.newBuilder()
        .setFileInfo(fi)
        .setTs(2345)
        .build();
    mTestMetastore.putDoraMeta(path, fs);
    Optional<DoraMeta.FileStatus> res = mTestMetastore.getDoraMeta(path);
    assert (res.isPresent());
    assert (res.get().equals(fs));
    System.out.println(res);

    mTestMetastore.removeDoraMeta(path);
    Optional<DoraMeta.FileStatus> res2 = mTestMetastore.getDoraMeta(path);
    assert (!res2.isPresent());

    System.out.println("End testPutAndGet");
  }

  public void testGetNotExist() {
    System.out.println("Start testGetNotExist");

    String path = new String("/EXIST");
    FileInfo fi = FileInfo.newBuilder()
        .setFileId(1234)
        .setMode(0567)
        .setLength(1000)
        .build();
    DoraMeta.FileStatus fs = DoraMeta.FileStatus.newBuilder()
        .setFileInfo(fi)
        .setTs(2345)
        .build();
    mTestMetastore.putDoraMeta(path, fs);
    String pathNotExist = new String("/NOT_EXIST");
    Optional<DoraMeta.FileStatus> res2 = mTestMetastore.getDoraMeta(pathNotExist);
    assert (!res2.isPresent());
    System.out.println(res2);

    System.out.println("End testGetNotExist");
  }

  public void testRemoveNotExist() {
    System.out.println("Start testRemoveNotExist");

    String pathNotExist = new String("/REMOVE_NOT_EXIST");
    mTestMetastore.removeDoraMeta(pathNotExist);

    System.out.println("End testRemoveNotExist");
  }

  /**
   * Performance Testing.
   */
  public void testPutAndGetMulti() {
    System.out.println("Start testPutAndGetMulti");
    // Please tune this to a reasonable number when doing performance evaluation.
    final int N = 100000;
    long start = System.currentTimeMillis();
    for (int i = 0; i < N; i++) {
      String path = new String("/HELLO" + i);

      long ts = System.currentTimeMillis();

      FileInfo fi = FileInfo.newBuilder()
          .setFileId(10000000 + i)
          .setMode(0567)
          .setLength(1000)
          .build();
      DoraMeta.FileStatus fs = DoraMeta.FileStatus.newBuilder()
          .setFileInfo(fi)
          .setTs(ts)
          .build();
      mTestMetastore.putDoraMeta(path, fs);
      Optional<DoraMeta.FileStatus> res = mTestMetastore.getDoraMeta(path);
      assert (res.isPresent());
      assert (res.get().equals(fs));
    }
    long end = System.currentTimeMillis();
    System.out.println("PutAndGet()/sec = " + N + "/" + (end - start) + "ms = "
        + (N * 1000 / (end - start)));
    Optional<Long> s = mTestMetastore.size();
    System.out.println("Number of key/value pairs = " + (s.isPresent() ? s.get() : 0));

    System.out.println("End testPutAndGetMulti");
  }
}
