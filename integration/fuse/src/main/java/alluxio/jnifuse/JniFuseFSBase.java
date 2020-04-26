/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 *
 */

package alluxio.jnifuse;

import java.nio.ByteBuffer;

public abstract class JniFuseFSBase {

  private native int fuse_main(int argc, String[] argv);

  public void mount(String[] fuseOpts) {
    fuse_main(fuseOpts.length, fuseOpts);
  }

  public void umount() {
    // TODO
  }

  // TODO: support more callbacks
  public abstract int open(String path, FileInfo fi);

  public abstract int read(String path, ByteBuffer buf, long size, long offset, FileInfo fi);

  public abstract int getattr(String path, FileStat stat);

  public abstract int readdir(String path, long bufaddr, FuseFiller filter, long offset,
      ByteBuffer fi);

  public int openCallback(String path, ByteBuffer buf) {
    FileInfo fi = new FileInfo(buf);
    return open(path, fi);
  }

  public int readCallback(String path, ByteBuffer buf, long size, long offset, ByteBuffer fibuf) {
    FileInfo fi = new FileInfo(fibuf);
    return read(path, buf, size, offset, fi);
  }

  public int getattrCallback(String path, ByteBuffer buf) {
    FileStat stat = new FileStat(buf);
    return getattr(path, stat);
  }

  public int readdirCallback(String path, long bufaddr, FuseFiller filter, long offset,
      ByteBuffer fi) {
    return readdir(path, bufaddr, filter, offset, fi);
  }

  static {
    System.loadLibrary("jnifuse");
  }
}
