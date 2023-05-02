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

package alluxio.jnifuse.struct;

import static org.junit.Assert.assertEquals;

import jnr.ffi.Runtime;
import org.junit.Test;

import java.nio.ByteBuffer;

public class FileStatTest {

  @Test
  public void offset() {
    // allocate an enough large memory for jnistat
    FileStat jnistat = FileStat.of(ByteBuffer.allocate(256));
    ru.serce.jnrfuse.struct.FileStat jnrstat =
        new ru.serce.jnrfuse.struct.FileStat(Runtime.getSystemRuntime());

    assertEquals(jnrstat.st_dev.offset(), jnistat.st_dev.offset());
    assertEquals(jnrstat.st_ino.offset(), jnistat.st_ino.offset());
    assertEquals(jnrstat.st_nlink.offset(), jnistat.st_nlink.offset());
    assertEquals(jnrstat.st_mode.offset(), jnistat.st_mode.offset());
    assertEquals(jnrstat.st_uid.offset(), jnistat.st_uid.offset());
    assertEquals(jnrstat.st_gid.offset(), jnistat.st_gid.offset());
    assertEquals(jnrstat.st_rdev.offset(), jnistat.st_rdev.offset());
    assertEquals(jnrstat.st_size.offset(), jnistat.st_size.offset());
    assertEquals(jnrstat.st_blksize.offset(), jnistat.st_blksize.offset());
    assertEquals(jnrstat.st_blocks.offset(), jnistat.st_blocks.offset());
    assertEquals(jnrstat.st_atim.tv_sec.offset(), jnistat.st_atim.tv_sec.offset());
    assertEquals(jnrstat.st_atim.tv_nsec.offset(), jnistat.st_atim.tv_nsec.offset());
    assertEquals(jnrstat.st_mtim.tv_sec.offset(), jnistat.st_mtim.tv_sec.offset());
    assertEquals(jnrstat.st_mtim.tv_nsec.offset(), jnistat.st_mtim.tv_nsec.offset());
    assertEquals(jnrstat.st_ctim.tv_sec.offset(), jnistat.st_ctim.tv_sec.offset());
    assertEquals(jnrstat.st_ctim.tv_nsec.offset(), jnistat.st_ctim.tv_nsec.offset());
  }

  @Test
  public void dataConsistency() {
    FileStat stat = FileStat.of(ByteBuffer.allocateDirect(256));
    int mode = FileStat.ALL_READ | FileStat.ALL_WRITE | FileStat.S_IFDIR;
    long size = 0x123456789888721L;
    stat.st_mode.set(mode);
    stat.st_size.set(size);
    assertEquals(mode, stat.st_mode.intValue());
    assertEquals(size, stat.st_size.longValue());

    ByteBuffer buf = stat.getBuffer();
    assertEquals(mode, buf.getShort((int) stat.st_mode.offset()));
    assertEquals(size, buf.getLong((int) stat.st_size.offset()));
  }
}
