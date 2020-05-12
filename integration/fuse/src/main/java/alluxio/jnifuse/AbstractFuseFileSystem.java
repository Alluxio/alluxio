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
import alluxio.jnifuse.struct.FuseFileInfo;
import alluxio.jnifuse.struct.Statvfs;
import alluxio.jnifuse.utils.SecurityUtils;
import alluxio.util.OSUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract class for other File System to extend and integrate with Fuse.
 */
public abstract class AbstractFuseFileSystem implements FuseFileSystem {

  static {
    System.loadLibrary("jnifuse");
  }

  private static final int TIMEOUT = 2000; // ms

  private final LibFuse libFuse;
  private final AtomicBoolean mounted = new AtomicBoolean();
  private final Path mountPoint;

  public AbstractFuseFileSystem(Path mountPoint) {
    this.libFuse = new LibFuse();
    this.mountPoint = mountPoint;
  }

  /**
   * Executes mount command.
   *
   * @param blocking whether this command is blocking
   * @param debug whether to show debug information
   * @param fuseOpts
   */
  public void mount(boolean blocking, boolean debug, String[] fuseOpts) {
    if (!mounted.compareAndSet(false, true)) {
      throw new FuseException("Fuse File System already mounted!");
    }
    String[] arg;
    String mountPointStr = mountPoint.toAbsolutePath().toString();
    if (mountPointStr.endsWith("\\")) {
      mountPointStr = mountPointStr.substring(0, mountPointStr.length() - 1);
    }
    if (!debug) {
      arg = new String[] {getFileSystemName(), "-f", mountPointStr};
    } else {
      arg = new String[] {getFileSystemName(), "-f", "-d", mountPointStr};
    }
    if (fuseOpts.length != 0) {
      int argLen = arg.length;
      arg = Arrays.copyOf(arg, argLen + fuseOpts.length);
      System.arraycopy(fuseOpts, 0, arg, argLen, fuseOpts.length);
    }

    final String[] args = arg;
    try {
      if (SecurityUtils.canHandleShutdownHooks()) {
        Runtime.getRuntime().addShutdownHook(new Thread(this::umount));
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
      String mountPath = mountPoint.toAbsolutePath().toString();
      try {
        new ProcessBuilder("fusermount", "-u", "-z", mountPath).start();
      } catch (IOException e) {
        try {
          new ProcessBuilder("umount", mountPath).start().waitFor();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new FuseException("Unable to umount FS", e);
        } catch (IOException ioe) {
          ioe.addSuppressed(e);
          throw new FuseException("Unable to umount FS", ioe);
        }
      }
    }
    mounted.set(false);
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

  public int unlinkCallback(String path) {
    return unlink(path);
  }

  public int flushCallback(String path, ByteBuffer fi) {
    return flush(path, FuseFileInfo.wrap(fi));
  }

  public int releaseCallback(String path, ByteBuffer fi) {
    return release(path, FuseFileInfo.wrap(fi));
  }

  public int chmodCallback(String path, long mode) {
    return chmod(path, mode);
  }

  public int chownCallback(String path, long uid, long gid) {
    return chown(path, uid, gid);
  }

  public int createCallback(String path, long mode, ByteBuffer fi) {
    return create(path, mode, FuseFileInfo.wrap(fi));
  }

  public int mkdirCallback(String path, long mode) {
    return mkdir(path, mode);
  }

  public int renameCallback(String oldPath, String newPath) {
    return rename(oldPath, newPath);
  }

  public int rmdirCallback(String path) {
    return rmdir(path);
  }

  public int statfsCallback(String path, ByteBuffer stbuf) {
    return statfs(path, Statvfs.wrap(stbuf));
  }

  public int truncateCallback(String path, long size) {
    return truncate(path, size);
  }

  public int writeCallback(String path, ByteBuffer buf, long size, long offset, ByteBuffer fi) {
    return write(path, buf, size, offset, FuseFileInfo.wrap(fi));
  }
}
