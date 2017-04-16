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

package alluxio.util;

import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.gcs.GCSUnderFileSystem;
import alluxio.underfs.hdfs.HdfsUnderFileSystem;
import alluxio.underfs.local.LocalUnderFileSystem;
import alluxio.underfs.oss.OSSUnderFileSystem;
import alluxio.underfs.s3.S3UnderFileSystem;
import alluxio.underfs.s3a.S3AUnderFileSystem;
import alluxio.underfs.swift.SwiftUnderFileSystem;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link UnderFileSystemUtils}.
 */
public class UnderFileSystemUtilsTest {
  /**
   * A callable which validates the type of an under file system. Should be used only in test as
   * part of {@link UfsTypeCheckPair}.
   */
  interface UfsTypeCheckCallable {
    boolean call(UnderFileSystem ufs);
  }

  /**
   * A pair consisting of a list of {@link UnderFileSystem}s which are the same type. Being the
   * same type is defined by all passing the {@link UfsTypeCheckCallable} associated with the pair.
   */
  private static final class UfsTypeCheckPair {
    private List<UnderFileSystem> mUfses;
    private UfsTypeCheckCallable mCallable;

    private UfsTypeCheckPair(List<UnderFileSystem> ufses, UfsTypeCheckCallable callable) {
      mUfses = ufses;
      mCallable = callable;
    }

    /**
     * @return the list of {@link UnderFileSystem}s in this pair
     */
    public List<UnderFileSystem> getUfses() {
      return mUfses;
    }

    /**
     * Checks if each element in the argument list satisfies the {@link UfsTypeCheckCallable}.
     * @param ufses list of {@link UnderFileSystem}s to check
     * @return true if all elements pass, false otherwise
     */
    public boolean checkUfs(List<UnderFileSystem> ufses) {
      for (UnderFileSystem ufs : ufses) {
        if (!mCallable.call(ufs)) {
          return false;
        }
      }
      return true;
    }
  }

  private static List<UfsTypeCheckPair> sPairs;
  private static List<UnderFileSystem> sObjectStores;

  @BeforeClass
  public static void beforeClass() {
    // For each UFS type, create a pair, add the pair to the object stores if necessary
    sPairs = new ArrayList<>();
    sObjectStores = new ArrayList<>();

    // GCS
    UnderFileSystem gcs = Mockito.mock(GCSUnderFileSystem.class);
    Mockito.when(gcs.getUnderFSType()).thenCallRealMethod();
    sPairs.add(new UfsTypeCheckPair(Collections.singletonList(gcs), new UfsTypeCheckCallable() {
      @Override
      public boolean call(UnderFileSystem ufs) {
        return UnderFileSystemUtils.isGcs(ufs);
      }
    }));
    sObjectStores.add(gcs);

    // HDFS
    UnderFileSystem hdfs = Mockito.mock(HdfsUnderFileSystem.class);
    Mockito.when(hdfs.getUnderFSType()).thenCallRealMethod();
    sPairs.add(new UfsTypeCheckPair(Collections.singletonList(hdfs), new UfsTypeCheckCallable() {
      @Override
      public boolean call(UnderFileSystem ufs) {
        return UnderFileSystemUtils.isHdfs(ufs);
      }
    }));

    // Local
    UnderFileSystem local = Mockito.mock(LocalUnderFileSystem.class);
    Mockito.when(local.getUnderFSType()).thenCallRealMethod();
    sPairs.add(new UfsTypeCheckPair(Collections.singletonList(local), new UfsTypeCheckCallable() {
      @Override
      public boolean call(UnderFileSystem ufs) {
        return UnderFileSystemUtils.isLocal(ufs);
      }
    }));

    // OSS
    UnderFileSystem oss = Mockito.mock(OSSUnderFileSystem.class);
    Mockito.when(oss.getUnderFSType()).thenCallRealMethod();
    sPairs.add(new UfsTypeCheckPair(Collections.singletonList(oss), new UfsTypeCheckCallable() {
      @Override
      public boolean call(UnderFileSystem ufs) {
        return UnderFileSystemUtils.isOss(ufs);
      }
    }));
    sObjectStores.add(oss);

    // S3
    UnderFileSystem s3 = Mockito.mock(S3UnderFileSystem.class);
    Mockito.when(s3.getUnderFSType()).thenCallRealMethod();
    UnderFileSystem s3a = Mockito.mock(S3AUnderFileSystem.class);
    Mockito.when(s3a.getUnderFSType()).thenCallRealMethod();
    sPairs.add(new UfsTypeCheckPair(Arrays.asList(s3, s3a), new UfsTypeCheckCallable() {
      @Override
      public boolean call(UnderFileSystem ufs) {
        return UnderFileSystemUtils.isS3(ufs);
      }
    }));
    sObjectStores.add(s3);
    sObjectStores.add(s3a);

    // Swift
    UnderFileSystem swift = Mockito.mock(SwiftUnderFileSystem.class);
    Mockito.when(swift.getUnderFSType()).thenCallRealMethod();
    sPairs.add(new UfsTypeCheckPair(Collections.singletonList(swift), new UfsTypeCheckCallable() {
      @Override
      public boolean call(UnderFileSystem ufs) {
        return UnderFileSystemUtils.isSwift(ufs);
      }
    }));
    sObjectStores.add(swift);
  }

  @Test
  public void typeCheck() {
    for (UfsTypeCheckPair ufs : sPairs) {
      for (UfsTypeCheckPair callable : sPairs) {
        Assert.assertEquals(callable.checkUfs(ufs.getUfses()), ufs.equals(callable));
      }
    }
  }

  @Test
  public void objectStoreCheck() {
    for (UnderFileSystem objectStore : sObjectStores) {
      Assert.assertTrue(UnderFileSystemUtils.isObjectStorage(objectStore));
    }
  }
}
