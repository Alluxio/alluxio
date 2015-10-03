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

package tachyon.master.file.meta;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.TachyonURI;
import tachyon.exception.TachyonException;

public class MountTableTest {
  private MountTable mMountTable;

  @Before
  public void before() {
    mMountTable = new MountTable();
  }

  @Test
  public void pathTest() throws TachyonException {
    // Test add()
    Assert.assertTrue(mMountTable.add(new TachyonURI("/"), new TachyonURI("/")));
    Assert.assertTrue(mMountTable.add(new TachyonURI("/mnt/foo"), new TachyonURI("/foo")));
    Assert.assertTrue(mMountTable.add(new TachyonURI("/mnt/foo2"), new TachyonURI("/foo/x")));
    Assert.assertFalse(mMountTable.add(new TachyonURI("/mnt/foo"), new TachyonURI("/foo2")));
    Assert.assertTrue(mMountTable.add(new TachyonURI("/mnt/bar"), new TachyonURI("/bar")));
    Assert.assertFalse(mMountTable.add(new TachyonURI("/mnt/bar/baz"), new TachyonURI("/baz")));

    // Test resolve()
    Assert.assertEquals(new TachyonURI("/foo"), mMountTable.resolve(new TachyonURI("/mnt/foo")));
    Assert
        .assertEquals(new TachyonURI("/foo/x"), mMountTable.resolve(new TachyonURI("/mnt/foo/x")));
    Assert.assertEquals(new TachyonURI("/bar"), mMountTable.resolve(new TachyonURI("/mnt/bar")));
    Assert
        .assertEquals(new TachyonURI("/bar/y"), mMountTable.resolve(new TachyonURI("/mnt/bar/y")));
    Assert.assertEquals(new TachyonURI("/bar/baz"),
        mMountTable.resolve(new TachyonURI("/mnt/bar/baz")));
    Assert.assertEquals(new TachyonURI("/mnt"), mMountTable.resolve(new TachyonURI("/mnt")));
    Assert.assertEquals(new TachyonURI("/foobar"), mMountTable.resolve(new TachyonURI("/foobar")));
    Assert.assertEquals(new TachyonURI("/"), mMountTable.resolve(new TachyonURI("/")));

    // Test getMountPoint()
    Assert.assertEquals("/mnt/foo", mMountTable.getMountPoint(new TachyonURI("/mnt/foo")));
    Assert.assertEquals("/mnt/foo", mMountTable.getMountPoint(new TachyonURI("/mnt/foo/x")));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(new TachyonURI("/mnt/bar")));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(new TachyonURI("/mnt/bar/y")));
    Assert.assertEquals("/mnt/bar", mMountTable.getMountPoint(new TachyonURI("/mnt/bar/baz")));
    Assert.assertEquals("/", mMountTable.getMountPoint(new TachyonURI("/mnt")));
    Assert.assertEquals("/", mMountTable.getMountPoint(new TachyonURI("/tmp")));
    Assert.assertEquals("/", mMountTable.getMountPoint(new TachyonURI("/")));

    // Test isMountPoint()
    Assert.assertTrue(mMountTable.isMountPoint(new TachyonURI("/")));
    Assert.assertTrue(mMountTable.isMountPoint(new TachyonURI("/mnt/foo")));
    Assert.assertFalse(mMountTable.isMountPoint(new TachyonURI("/mnt/foo/bar")));
    Assert.assertFalse(mMountTable.isMountPoint(new TachyonURI("/mnt")));
    Assert.assertTrue(mMountTable.isMountPoint(new TachyonURI("/mnt/foo2")));
    Assert.assertFalse(mMountTable.isMountPoint(new TachyonURI("/mnt/foo3")));
    Assert.assertTrue(mMountTable.isMountPoint(new TachyonURI("/mnt/bar")));
    Assert.assertFalse(mMountTable.isMountPoint(new TachyonURI("/mnt/bar/baz")));

    // Test delete()
    Assert.assertTrue(mMountTable.delete(new TachyonURI("/mnt/bar")));
    Assert.assertTrue(mMountTable.delete(new TachyonURI("/mnt/foo")));
    Assert.assertTrue(mMountTable.delete(new TachyonURI("/mnt/foo2")));
    Assert.assertFalse(mMountTable.delete(new TachyonURI("/mnt/foo")));
    Assert.assertFalse(mMountTable.delete(new TachyonURI("/")));
  }

  @Test
  public void uriTest() throws TachyonException {
    // Test add()
    Assert.assertTrue(mMountTable.add(new TachyonURI("tachyon://localhost:1234/"),
        new TachyonURI("hdfs://localhost:5678/")));
    Assert.assertTrue(mMountTable.add(new TachyonURI("tachyon://localhost:1234/mnt/foo"),
        new TachyonURI("hdfs://localhost:5678/foo")));
    Assert.assertTrue(mMountTable.add(new TachyonURI("tachyon://localhost:1234/mnt/foo2"),
        new TachyonURI("hdfs://localhost:5678/foo/x")));
    Assert.assertFalse(mMountTable.add(new TachyonURI("tachyon://localhost:1234/mnt/foo"),
        new TachyonURI("hdfs://localhost:5678/foo2")));
    Assert.assertTrue(mMountTable.add(new TachyonURI("tachyon://localhost:1234/mnt/bar"),
        new TachyonURI("hdfs://localhost:5678/bar")));
    Assert.assertFalse(mMountTable.add(new TachyonURI("tachyon://localhost:1234/mnt/bar/baz"),
        new TachyonURI("hdfs://localhost:5678/baz")));

    // Test resolve()
    Assert.assertEquals(new TachyonURI("hdfs://localhost:5678/foo"),
        mMountTable.resolve(new TachyonURI("tachyon://localhost:1234/mnt/foo")));
    Assert.assertEquals(new TachyonURI("hdfs://localhost:5678/foo/x"),
        mMountTable.resolve(new TachyonURI("tachyon://localhost:1234/mnt/foo/x")));
    Assert.assertEquals(new TachyonURI("hdfs://localhost:5678/bar"),
        mMountTable.resolve(new TachyonURI("tachyon://localhost:1234/mnt/bar")));
    Assert.assertEquals(new TachyonURI("hdfs://localhost:5678/bar/y"),
        mMountTable.resolve(new TachyonURI("tachyon://localhost:1234/mnt/bar/y")));
    Assert.assertEquals(new TachyonURI("hdfs://localhost:5678/bar/baz"),
        mMountTable.resolve(new TachyonURI("tachyon://localhost:1234/mnt/bar/baz")));
    Assert.assertEquals(new TachyonURI("hdfs://localhost:5678/mnt"),
        mMountTable.resolve(new TachyonURI("tachyon://localhost:1234/mnt")));
    Assert.assertEquals(new TachyonURI("hdfs://localhost:5678/foobar"),
        mMountTable.resolve(new TachyonURI("tachyon://localhost:1234/foobar")));
    Assert.assertEquals(new TachyonURI("hdfs://localhost:5678/"),
        mMountTable.resolve(new TachyonURI("tachyon://localhost:1234/")));

    // Test getMountPoint()
    Assert.assertEquals("/mnt/foo",
        mMountTable.getMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/foo")));
    Assert.assertEquals("/mnt/foo",
        mMountTable.getMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/foo/x")));
    Assert.assertEquals("/mnt/bar",
        mMountTable.getMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/bar")));
    Assert.assertEquals("/mnt/bar",
        mMountTable.getMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/bar/y")));
    Assert.assertEquals("/mnt/bar",
        mMountTable.getMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/bar/baz")));
    Assert.assertEquals("/",
        mMountTable.getMountPoint(new TachyonURI("tachyon://localhost:1234/mnt")));
    Assert.assertEquals("/",
        mMountTable.getMountPoint(new TachyonURI("tachyon://localhost:1234/tmp")));
    Assert
        .assertEquals("/", mMountTable.getMountPoint(new TachyonURI("tachyon://localhost:1234/")));

    // Test isMountPoint()
    Assert.assertTrue(mMountTable.isMountPoint(new TachyonURI("tachyon://localhost:1234/")));
    Assert.assertTrue(mMountTable.isMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/foo")));
    Assert.assertFalse(mMountTable.isMountPoint(new TachyonURI(
        "tachyon://localhost:1234/mnt/foo/bar")));
    Assert.assertFalse(mMountTable.isMountPoint(new TachyonURI("tachyon://localhost:1234/mnt")));
    Assert
        .assertTrue(mMountTable.isMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/foo2")));
    Assert.assertFalse(mMountTable
        .isMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/foo3")));
    Assert.assertTrue(mMountTable.isMountPoint(new TachyonURI("tachyon://localhost:1234/mnt/bar")));
    Assert.assertFalse(mMountTable.isMountPoint(new TachyonURI(
        "tachyon://localhost:1234/mnt/bar/baz")));

    // Test delete()
    Assert.assertTrue(mMountTable.delete(new TachyonURI("tachyon://localhost:1234/mnt/bar")));
    Assert.assertTrue(mMountTable.delete(new TachyonURI("tachyon://localhost:1234/mnt/foo")));
    Assert.assertTrue(mMountTable.delete(new TachyonURI("tachyon://localhost:1234/mnt/foo2")));
    Assert.assertFalse(mMountTable.delete(new TachyonURI("tachyon://localhost:1234/mnt/foo")));
    Assert.assertFalse(mMountTable.delete(new TachyonURI("tachyon://localhost:1234/")));
  }
}
