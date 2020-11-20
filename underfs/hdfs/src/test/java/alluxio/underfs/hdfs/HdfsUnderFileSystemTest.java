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

package alluxio.underfs.hdfs;

import static alluxio.underfs.hdfs.HdfsPositionedUnderFileInputStream.MOVEMENT_LIMIT;
import static alluxio.underfs.hdfs.HdfsPositionedUnderFileInputStream.SEQUENTIAL_READ_LIMIT;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import alluxio.AlluxioURI;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.SeekableUnderFileInputStream;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.powermock.reflect.Whitebox;

import java.io.File;

/**
 * Tests {@link HdfsUnderFileSystem}.
 */
public final class HdfsUnderFileSystemTest {

  private HdfsUnderFileSystem mHdfsUnderFileSystem;
  private final AlluxioConfiguration mAlluxioConf = ConfigurationTestUtils.defaults();

  @Rule
  public TemporaryFolder mTemporaryFolder = new TemporaryFolder();

  @Before
  public final void before() throws Exception {
    UnderFileSystemConfiguration conf =
        UnderFileSystemConfiguration.defaults(ConfigurationTestUtils.defaults())
            .createMountSpecificConf(ImmutableMap.of("hadoop.security.group.mapping",
                "org.apache.hadoop.security.ShellBasedUnixGroupsMapping", "fs.hdfs.impl",
            PropertyKey.UNDERFS_HDFS_IMPL.getDefaultValue()));
    mHdfsUnderFileSystem = HdfsUnderFileSystem.createInstance(
        new AlluxioURI(mTemporaryFolder.getRoot().getAbsolutePath()), conf);
  }

  /**
   * Tests the {@link HdfsUnderFileSystem#getUnderFSType()} method. Confirm the UnderFSType for
   * HdfsUnderFileSystem.
   */
  @Test
  public void getUnderFSType() throws Exception {
    Assert.assertEquals("hdfs", mHdfsUnderFileSystem.getUnderFSType());
  }

  /**
   * Tests the {@link HdfsUnderFileSystem#createConfiguration} method.
   *
   * Checks the hdfs implements class and alluxio underfs config setting
   */
  @Test
  public void prepareConfiguration() throws Exception {
    UnderFileSystemConfiguration ufsConf =
        UnderFileSystemConfiguration.defaults(ConfigurationTestUtils.defaults());
    org.apache.hadoop.conf.Configuration conf = HdfsUnderFileSystem.createConfiguration(ufsConf);
    Assert.assertEquals(ufsConf.get(PropertyKey.UNDERFS_HDFS_IMPL), conf.get("fs.hdfs.impl"));
    Assert.assertTrue(conf.getBoolean("fs.hdfs.impl.disable.cache", false));
  }

  private void checkDataValid(int data, int index) {
    // index is larger than Byte.MAX_VALUE, convert to byte to compare
    Assert.assertEquals((byte) index, (byte) data);
  }

  /**
   * Tests the dynamic switching between pread and read calls to underlying stream.
   */
  @Test
  public void verifyPread() throws Exception {
    File file = mTemporaryFolder.newFile("test.txt");
    byte[] data = new byte[4 * MOVEMENT_LIMIT];
    for (int i = 0; i < 4 * MOVEMENT_LIMIT; i++) {
      data[i] = (byte) i;
    }
    FileUtils.writeByteArrayToFile(file, data);
    SeekableUnderFileInputStream in = (SeekableUnderFileInputStream) mHdfsUnderFileSystem.open(
        file.getAbsolutePath(), OpenOptions.defaults().setPositionShort(true));
    FSDataInputStream dataInput = Whitebox.getInternalState(in, "in");
    PreadSeekableStream stream = new PreadSeekableStream(dataInput);
    PreadSeekableStream spyStream = spy(stream);
    Whitebox.setInternalState(in, "in", spyStream);
    int readPos = 0;
    checkDataValid(in.read(), readPos++);
    checkDataValid(in.read(), readPos++);
    checkDataValid(in.read(), readPos++);
    checkDataValid(in.read(), readPos++);
    in.skip(2);
    in.skip(2);
    readPos += 4;
    checkDataValid(in.read(), readPos++);
    in.skip(2);
    readPos += 2;
    checkDataValid(in.read(), readPos++);
    // we are in sequential read mode, therefore all reads are normal reads and never preads
    verify(spyStream, never()).read(anyInt(), any(byte[].class), anyInt(), anyInt());
    verify(spyStream, times(6)).read(any(byte[].class), anyInt(), anyInt());
    in.skip(MOVEMENT_LIMIT + 1);
    readPos += MOVEMENT_LIMIT + 1;
    for (int i = 0; i < SEQUENTIAL_READ_LIMIT; i++) {
      checkDataValid(in.read(), readPos++);
    }
    // because we skipped over more than MOVEMENT_LIMIT, we switched to pread mode, the next
    // three reads are preads
    verify(spyStream, times(SEQUENTIAL_READ_LIMIT)).read(
        anyLong(), any(byte[].class), anyInt(), anyInt());
    verify(spyStream, times(6)).read(any(byte[].class), anyInt(), anyInt());
    // we performed more than SEQUENTIAL_READ_LIMIT reads without seeking beyond movement limit,
    // thus we switch back to sequential read mode
    checkDataValid(in.read(), readPos++);
    verify(spyStream, times(SEQUENTIAL_READ_LIMIT)).read(
        anyLong(), any(byte[].class), anyInt(), anyInt());
    verify(spyStream, times(7)).read(any(byte[].class), anyInt(), anyInt());
    in.seek(MOVEMENT_LIMIT * 3);
    readPos = MOVEMENT_LIMIT * 3;
    checkDataValid(in.read(), readPos++);
    // we performed seek to a far location, we should switch back to pread mode
    verify(spyStream, times(SEQUENTIAL_READ_LIMIT + 1)).read(
        anyLong(), any(byte[].class), anyInt(), anyInt());
    verify(spyStream, times(7)).read(any(byte[].class), anyInt(), anyInt());
    in.close();
  }
}
