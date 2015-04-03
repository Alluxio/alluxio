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
package tachyon;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tachyon.conf.TachyonConf;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

/**
 * Unit tests for {@link UnderFileSystemDummy}
 */
public final class UnderFileSystemDummyTest {
  private TachyonConf mTachyonConf;
  private UnderFileSystemDummy mUFSD;
  @Before
  public void before() {
    mTachyonConf = new TachyonConf();
    mUFSD = new UnderFileSystemDummy(mTachyonConf);
  }

  @Test
  public void createTest() throws IOException {
    Assert.assertTrue(mUFSD.create("") instanceof DummyFileOutputStream);
    Assert.assertTrue(mUFSD.create("",0) instanceof DummyFileOutputStream);
    Assert.assertTrue(mUFSD.create("",(short)0,0) instanceof DummyFileOutputStream);
  }

  @Test
  public void deleteTest() throws IOException {
    Assert.assertTrue(mUFSD.delete("", true));
    Assert.assertTrue(mUFSD.delete("", false));
  }

  @Test
  public void existsTest() throws IOException {
    Assert.assertFalse(mUFSD.exists(""));
  }

  @Test
  public void getBlockSizeByteTest() throws IOException {
    Assert.assertEquals(0, mUFSD.getBlockSizeByte(""));
  }

  @Test
  public void getConfTest() throws IOException {
    Assert.assertTrue(mUFSD.getConf() == null);
  }

  @Test
  public void getFileLocationsTest() throws IOException {
    Assert.assertTrue(mUFSD.getFileLocations("").isEmpty());
    Assert.assertTrue(mUFSD.getFileLocations("", 0).isEmpty());
  }

  @Test
  public void getFileSizeTest() throws IOException {
    Assert.assertEquals(0, mUFSD.getFileSize(""));
  }

  @Test
  public void getModificationTimeMsTest() throws IOException {
    Assert.assertEquals(0, mUFSD.getModificationTimeMs(""));
  }

  @Test
  public void getSpaceTest() throws IOException {
    Assert.assertEquals(0, mUFSD.getSpace("", null));
  }

  @Test
  public void isFileTest() throws IOException {
    Assert.assertTrue(mUFSD.isFile(""));
  }

  @Test
  public void listTest() throws IOException {
    Assert.assertEquals(0, mUFSD.list("").length);
  }

  @Test
  public void mkdirsTest() throws IOException {
    Assert.assertTrue(mUFSD.mkdirs("", true));
    Assert.assertTrue(mUFSD.mkdirs("", false));
  }

  @Test
  public void openTest() throws IOException {
    Assert.assertTrue(mUFSD.open("") instanceof DummyFileInputStream);
  }

  @Test
  public void renameTest() throws IOException {
    Assert.assertTrue(mUFSD.rename("",""));
  }
}
