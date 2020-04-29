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

package alluxio.jnifuse;

import alluxio.jnifuse.struct.FileStat;
import alluxio.jnifuse.struct.FuseContext;
import alluxio.jnifuse.struct.FuseFileInfo;
import alluxio.jnifuse.struct.Statvfs;
import alluxio.util.OSUtils;

import org.apache.commons.lang.NotImplementedException;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseException;
import ru.serce.jnrfuse.utils.MountUtils;
import ru.serce.jnrfuse.utils.SecurityUtils;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract class for Fuse FS Stub.
 */
public abstract class FuseStubFS {

  private static final int TIMEOUT = 2000; // ms
  private static final String[] osxFuseLibraries = {"fuse4x", "osxfuse", "macfuse", "fuse"};

  private Set<String> notImplementedMethods;
  protected final LibFuse libFuse;
  // protected final FuseOperations fuseOperations;
  protected final AtomicBoolean mounted = new AtomicBoolean();
  protected Path mountPoint;
  // private volatile Pointer fusePointer;

  public FuseStubFS() {
    this.libFuse = new LibFuse();
  }

  public void mount(Path mountPoint, boolean blocking, boolean debug, String[] fuseOpts) {
    if (!mounted.compareAndSet(false, true)) {
      throw new FuseException("Fuse fs already mounted!");
    }
    this.mountPoint = mountPoint;
    String[] arg;
    String mountPointStr = mountPoint.toAbsolutePath().toString();
    if (mountPointStr.endsWith("\\")) {
      mountPointStr = mountPointStr.substring(0, mountPointStr.length() - 1);
    }
    if (!debug) {
      arg = new String[] {getFSName(), "-f", mountPointStr};
    } else {
      arg = new String[] {getFSName(), "-f", "-d", mountPointStr};
    }
    if (fuseOpts.length != 0) {
      int argLen = arg.length;
      arg = Arrays.copyOf(arg, argLen + fuseOpts.length);
      System.arraycopy(fuseOpts, 0, arg, argLen, fuseOpts.length);
    }

    final String[] args = arg;
    try {
      if (SecurityUtils.canHandleShutdownHooks()) {
        java.lang.Runtime.getRuntime().addShutdownHook(new Thread(this::umount));
      }
      int res;
      if (blocking) {
        res = execMount(args);
      } else {
        try {
          res = CompletableFuture.supplyAsync(() -> execMount(args)).get(TIMEOUT,
              TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          // ok
          res = 0;
        }
      }
      if (res != 0) {
        throw new FuseException("Unable to mount FS, return code = " + res);
      }
    } catch (Exception e) {
      mounted.set(false);
      throw new FuseException("Unable to mount FS", e);
    }
  }

  private int execMount(String[] arg) {
    return libFuse.fuse_main_real(this, arg.length, arg);
  }

  public void umount() {
    if (!mounted.get()) {
      return;
    }
    if (OSUtils.isWindows()) {
      // Pointer fusePointer = this.fusePointer;
      // if (fusePointer != null) {
      // libFuse.fuse_exit(fusePointer);
      // }
    } else {
      MountUtils.umount(mountPoint);
    }
    mounted.set(false);
  }

  public int getattr(String path, FileStat stat) {
    throw new NotImplementedException("getattr");
    // return 0;
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

  public int readdir(String path, long bufaddr, FuseFillDir filter, long offset, FuseFileInfo fi) {
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
    return readdir(path, bufaddr, filter, offset, new FuseFileInfo(fi));
  }

  public FuseContext getContext() {
    // TODO: get real context
    return new FuseContext(ByteBuffer.allocate(32));
  }

  protected String getFSName() {
    return "fusefs" + ThreadLocalRandom.current().nextInt();
  }

  static {
    System.loadLibrary("jnifuse");
  }
}
