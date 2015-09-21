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

import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.TachyonURI;
import tachyon.thrift.InvalidPathException;

public class MountTableTest {
  private MountTable mMountTable;

  @Before
  public void before() {
    mMountTable = new MountTable();
  }

  @Test
  public void mountTableTest() throws InvalidPathException {
    // Test add().
    Assert.assertTrue(mMountTable.add(new TachyonURI("/mnt/foo"), new TachyonURI("/foo")));
    Assert.assertTrue(mMountTable.add(new TachyonURI("/mnt/foo2"), new TachyonURI("/foo/x")));
    Assert.assertFalse(mMountTable.add(new TachyonURI("/mnt/foo"), new TachyonURI("/foo2")));
    Assert.assertTrue(mMountTable.add(new TachyonURI("/mnt/bar"), new TachyonURI("/bar")));
    Assert.assertFalse(mMountTable.add(new TachyonURI("/mnt/bar/x"), new TachyonURI("/bar")));

    // Test resolve().
    Assert.assertEquals(mMountTable.resolve(new TachyonURI("/mnt/foo")), new TachyonURI("/foo"));
    Assert.assertEquals(
        mMountTable.resolve(new TachyonURI("/mnt/foo/x")), new TachyonURI("/foo/x"));
    Assert.assertEquals(mMountTable.resolve(new TachyonURI("/mnt/bar")), new TachyonURI("/bar"));
    Assert.assertEquals(
        mMountTable.resolve(new TachyonURI("/mnt/bar/y")), new TachyonURI("/bar/y"));
    Assert.assertEquals(mMountTable.resolve(new TachyonURI("/mnt")), new TachyonURI("/mnt"));
    Assert.assertEquals(mMountTable.resolve(new TachyonURI("/tmp")), new TachyonURI("/tmp"));
    Assert.assertEquals(mMountTable.resolve(new TachyonURI("/")), new TachyonURI("/"));

    // Test getMountPoint().
    Assert.assertEquals(
        mMountTable.getMountPoint(new TachyonURI("/mnt/foo")), new TachyonURI("/mnt/foo"));
    Assert.assertEquals(
        mMountTable.getMountPoint(new TachyonURI("/mnt/foo/x")), new TachyonURI("/mnt/foo"));
    Assert.assertEquals(
        mMountTable.getMountPoint(new TachyonURI("/mnt/bar")), new TachyonURI("/mnt/bar"));
    Assert.assertEquals(
        mMountTable.getMountPoint(new TachyonURI("/mnt/bar/y")), new TachyonURI("/mnt/bar"));
    Assert.assertEquals(mMountTable.getMountPoint(new TachyonURI("/mnt")), new TachyonURI(""));
    Assert.assertEquals(mMountTable.getMountPoint(new TachyonURI("/tmp")), new TachyonURI(""));
    Assert.assertEquals(mMountTable.getMountPoint(new TachyonURI("/")), new TachyonURI(""));

    // Test delete().
    Assert.assertFalse(mMountTable.delete(new TachyonURI("/mnt/bar/x")));
    Assert.assertTrue(mMountTable.delete(new TachyonURI("/mnt/bar")));
    Assert.assertTrue(mMountTable.delete(new TachyonURI("/mnt/foo")));
    Assert.assertTrue(mMountTable.delete(new TachyonURI("/mnt/foo2")));
    Assert.assertFalse(mMountTable.delete(new TachyonURI("/mnt/foo")));
  }
}
