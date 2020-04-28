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

import alluxio.jnifuse.struct.FileStat;
import alluxio.jnifuse.struct.FuseFileInfo;
import alluxio.jnifuse.struct.Statvfs;
import ru.serce.jnrfuse.ErrorCodes;

import java.nio.ByteBuffer;

public abstract class FuseStubFS {

    private native int fuse_main(int argc, String[] argv);

    public void mount(String[] fuseOpts) {
        fuse_main(fuseOpts.length, fuseOpts);
    }

    public void umount() {
        // TODO
    }

    public int getattr(String path, FileStat stat) {
        return 0;
    }

    public int mkdir(String path, long mode) {
        return 0;
    }

    public int unlink(String path) {
        return 0;
    }

    public int rmdir(String path) {
        return 0;
    }

    public int symlink(String oldpath, String newpath) {
        return 0;
    }

    public int rename(String oldpath, String newpath) {
        return 0;
    }

    public int link(String oldpath, String newpath) {
        return 0;
    }

    public int chmod(String path, long mode) {
        return 0;
    }

    public int chown(String path, long uid, long gid) {
        return 0;
    }

    public int truncate(String path, long size) {
        return 0;
    }

    public int open(String path, FuseFileInfo fi) {
        return 0;
    }

    public int read(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
        return 0;
    }

    public int write(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
        return 0;
    }

    public int statfs(String path, Statvfs stbuf) {
        return 0;
    }

    public int flush(String path, FuseFileInfo fi) {
        return 0;
    }

    public int release(String path, FuseFileInfo fi) {
        return 0;
    }

    public int opendir(String path, FuseFileInfo fi) {
        return 0;
    }

    public int readdir(String path, long bufaddr, FuseFillDir filter, long offset,
                       ByteBuffer fi) {
        return 0;
    }

    public int releasedir(String path, FuseFileInfo fi) {
        return 0;
    }

    public int create(String path, long mode, FuseFileInfo fi) {
        return -ErrorCodes.ENOSYS();
    }

    public int openCallback(String path, ByteBuffer buf) {
        FuseFileInfo fi = new FuseFileInfo(buf);
        return open(path, fi);
    }

    public int readCallback(String path, ByteBuffer buf, long size, long offset, ByteBuffer fibuf) {
        FuseFileInfo fi = new FuseFileInfo(fibuf);
        return read(path, buf, size, offset, fi);
    }

    public int getattrCallback(String path, ByteBuffer buf) {
        FileStat stat = new FileStat(buf);
        return getattr(path, stat);
    }

    public int readdirCallback(String path, long bufaddr, FuseFillDir filter, long offset,
                               ByteBuffer fi) {
        return readdir(path, bufaddr, filter, offset, fi);
    }

    static {
        System.loadLibrary("jnifuse");
    }
}
