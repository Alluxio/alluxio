/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import tachyon.master.next.LocalTachyonCluster;

/**
 * Integration tests for TFS rename.
 */
public class TFSRenameIntegrationTest {

  private static final int BLOCK_SIZE = 1024;
  private static LocalTachyonCluster sLocalTachyonCluster;
  private static FileSystem sTFS;

  private static void create(FileSystem fs, Path path) throws IOException {
    FSDataOutputStream o = fs.create(path);
    o.writeBytes("Test Bytes");
    o.close();
  }

  public static void cleanup(FileSystem fs) throws IOException {
    FileStatus[] statuses = fs.listStatus(new Path("/"));
    for (FileStatus f : statuses) {
      fs.delete(f.getPath(), true);
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.tachyon.impl", TFS.class.getName());

    // Start local Tachyon cluster
    sLocalTachyonCluster = new LocalTachyonCluster(100000000, 100000, BLOCK_SIZE);
    sLocalTachyonCluster.start();
    URI uri = URI.create(sLocalTachyonCluster.getMasterUri());

    sTFS = FileSystem.get(uri, conf);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    sLocalTachyonCluster.stop();
  }

  @Test
  public void basicRenameTest() throws Exception {
    // Rename /fileA to /fileB
    {
      Path fileA = new Path("/fileA");
      Path fileB = new Path("/fileB");

      create(sTFS, fileA);

      Assert.assertTrue(sTFS.rename(fileA, fileB));

      Assert.assertFalse(sTFS.exists(fileA));
      Assert.assertTrue(sTFS.exists(fileB));
      cleanup(sTFS);
    }
    // Rename /fileA to /dirA/fileA
    {
      Path fileA = new Path("/fileA");
      Path dirA = new Path("/dirA");
      Path finalDst = new Path("/dirA/fileA");

      create(sTFS, fileA);
      sTFS.mkdirs(dirA);

      Assert.assertTrue(sTFS.rename(fileA, finalDst));

      Assert.assertFalse(sTFS.exists(fileA));
      Assert.assertTrue(sTFS.exists(dirA));
      Assert.assertTrue(sTFS.exists(finalDst));
      cleanup(sTFS);
    }
    // Rename /fileA to /dirA/fileA without specifying the full path
    {
      Path fileA = new Path("/fileA");
      Path dirA = new Path("/dirA");
      Path finalDst = new Path("/dirA/fileA");

      create(sTFS, fileA);
      sTFS.mkdirs(dirA);

      Assert.assertTrue(sTFS.rename(fileA, dirA));

      Assert.assertFalse(sTFS.exists(fileA));
      Assert.assertTrue(sTFS.exists(dirA));
      Assert.assertTrue(sTFS.exists(finalDst));
      cleanup(sTFS);
    }
    // Rename /fileA to /fileA
    {
      Path fileA = new Path("/fileA");

      create(sTFS, fileA);

      Assert.assertTrue(sTFS.rename(fileA, fileA));

      Assert.assertTrue(sTFS.exists(fileA));
      cleanup(sTFS);
    }
    // Rename /fileA to /fileAfileA
    {
      Path fileA = new Path("/fileA");
      Path finalDst = new Path("/fileAfileA");

      create(sTFS, fileA);

      Assert.assertTrue(sTFS.rename(fileA, finalDst));

      Assert.assertTrue(sTFS.exists(finalDst));
      cleanup(sTFS);
    }
    // Rename /dirA to /dirB, /dirA/fileA should become /dirB/fileA
    {
      Path dirA = new Path("/dirA");
      Path dirB = new Path("/dirB");
      Path fileA = new Path("/dirA/fileA");
      Path finalDst = new Path("/dirB/fileA");

      sTFS.mkdirs(dirA);
      create(sTFS, fileA);

      Assert.assertTrue(sTFS.rename(dirA, dirB));

      Assert.assertFalse(sTFS.exists(dirA));
      Assert.assertFalse(sTFS.exists(fileA));
      Assert.assertTrue(sTFS.exists(dirB));
      Assert.assertTrue(sTFS.exists(finalDst));
      cleanup(sTFS);
    }
    // Rename /dirA to /dirB, /dirA/fileA should become /dirB/fileA even if it was not closed
    {
      Path dirA = new Path("/dirA");
      Path dirB = new Path("/dirB");
      Path fileA = new Path("/dirA/fileA");
      Path finalDst = new Path("/dirB/fileA");

      sTFS.mkdirs(dirA);
      FSDataOutputStream o = sTFS.create(fileA);
      o.writeBytes("Test Bytes");
      o.sync();

      Assert.assertTrue(sTFS.rename(dirA, dirB));

      Assert.assertFalse(sTFS.exists(dirA));
      Assert.assertFalse(sTFS.exists(fileA));
      Assert.assertTrue(sTFS.exists(dirB));
      Assert.assertTrue(sTFS.exists(finalDst));

      o.close();

      Assert.assertFalse(sTFS.exists(dirA));
      Assert.assertFalse(sTFS.exists(fileA));
      Assert.assertTrue(sTFS.exists(dirB));
      Assert.assertTrue(sTFS.exists(finalDst));
      cleanup(sTFS);
    }
  }

  @Test
  public void errorRenameTest() throws Exception {
    // Rename /dirA to /dirA/dirB should fail
    {
      Path dirA = new Path("/dirA");
      Path finalDst = new Path("/dirA/dirB");

      sTFS.mkdirs(dirA);

      Assert.assertFalse(sTFS.rename(dirA, finalDst));

      Assert.assertFalse(sTFS.exists(finalDst));
      Assert.assertTrue(sTFS.exists(dirA));
      cleanup(sTFS);
    }
    // Rename /fileA to /fileB should fail if /fileB exists
    {
      Path fileA = new Path("/fileA");
      Path fileB = new Path("/fileB");

      create(sTFS, fileA);
      create(sTFS, fileB);

      Assert.assertFalse(sTFS.rename(fileA, fileB));

      Assert.assertTrue(sTFS.exists(fileA));
      Assert.assertTrue(sTFS.exists(fileB));
      cleanup(sTFS);
    }
    // Rename /fileA to /dirA/fileA should fail if /dirA/fileA exists
    {
      Path fileA = new Path("/fileA");
      Path dirA = new Path("/dirA");
      Path finalDst = new Path("/dirA/fileA");

      create(sTFS, fileA);
      create(sTFS, finalDst);
      sTFS.mkdirs(dirA);

      Assert.assertFalse(sTFS.rename(fileA, dirA));

      Assert.assertTrue(sTFS.exists(fileA));
      Assert.assertTrue(sTFS.exists(dirA));
      Assert.assertTrue(sTFS.exists(finalDst));
      cleanup(sTFS);
    }
    // Rename /fileA to an nonexistent path should fail
    {
      Path fileA = new Path("/fileA");
      Path nonexistentPath = new Path("/doesNotExist/fileA");

      create(sTFS, fileA);

      Assert.assertFalse(sTFS.rename(fileA, nonexistentPath));

      Assert.assertTrue(sTFS.exists(fileA));
      cleanup(sTFS);
    }
  }

}
