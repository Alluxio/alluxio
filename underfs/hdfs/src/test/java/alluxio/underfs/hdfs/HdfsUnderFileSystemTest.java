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
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

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

  class PreadSeekableStream extends FilterInputStream implements Seekable, PositionedReadable {

    /**
     * Creates a <code>FilterInputStream</code>
     * by assigning the  argument <code>in</code>
     * to the field <code>this.in</code> so as
     * to remember it for later use.
     *
     * @param in the underlying input stream, or <code>null</code> if
     *           this instance is to be created without an underlying stream.
     */
    protected PreadSeekableStream(InputStream in) {
      super(in);
    }

    @Override
    public int read(long l, byte[] bytes, int i, int i1) throws IOException {
      return ((FSDataInputStream) in).read(l, bytes, i, i1);
    }

    @Override
    public void readFully(long l, byte[] bytes, int i, int i1) throws IOException {
      ((FSDataInputStream) in).readFully(l, bytes, i, i1);
    }

    @Override
    public void readFully(long l, byte[] bytes) throws IOException {
      ((FSDataInputStream) in).readFully(l, bytes);
    }

    @Override
    public void seek(long l) throws IOException {
      ((FSDataInputStream) in).seek(l);
    }

    @Override
    public long getPos() throws IOException {
      return ((FSDataInputStream) in).getPos();
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
      return ((FSDataInputStream) in).seekToNewSource(l);
    }
  }
  /**
   * Tests the dynamic switching between pread and read calls to underlying stream.
   */
  @Test
  public void verifyPread() throws Exception {
    File file = mTemporaryFolder.newFile("test.txt");
    FileUtils.writeByteArrayToFile(file, CommonUtils.randomBytes(4096));
    FilterInputStream in = (FilterInputStream) mHdfsUnderFileSystem.open(file.getAbsolutePath(),
        OpenOptions.defaults().setPositionShort(true));
    FSDataInputStream dataInput = Whitebox.getInternalState(in, "in");
    PreadSeekableStream stream = new PreadSeekableStream(dataInput);
    PreadSeekableStream spyStream = spy(stream);
    Whitebox.setInternalState(in, "in", spyStream);
    in.read();
    in.read();
    in.read();
    in.read();
    in.skip(2);
    in.skip(2);
    in.read();
    in.skip(2);
    in.read();
    verify(spyStream, never()).read(anyInt(), any(byte[].class), anyInt(), anyInt());
    verify(spyStream, times(6)).read(any(byte[].class), anyInt(), anyInt());
    in.skip(1000);
    in.read();
    in.read();
    in.read();
    verify(spyStream, times(3)).read(anyInt(), any(byte[].class), anyInt(), anyInt());
    verify(spyStream, times(6)).read(any(byte[].class), anyInt(), anyInt());
    in.read();
    verify(spyStream, times(3)).read(anyInt(), any(byte[].class), anyInt(), anyInt());
    verify(spyStream, times(7)).read(any(byte[].class), anyInt(), anyInt());
    in.close();
  }
}
