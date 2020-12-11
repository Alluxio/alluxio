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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFuseFileSystem.class);
  // timeout to mount a JNI fuse file system in ms
  private static final int MOUNT_TIMEOUT_MS = 2000;

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
          res = CompletableFuture.supplyAsync(() -> execMount(args)).get(MOUNT_TIMEOUT_MS,
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
    try {
      return open(path, FuseFileInfo.wrap(buf));
    } catch (Exception e) {
      LOG.error("Failed to open {}: ", path, e);
      return -ErrorCodes.EIO();
    }
  }

  public int readCallback(String path, ByteBuffer buf, long size, long offset, ByteBuffer fibuf) {
    try {
      return read(path, buf, size, offset, FuseFileInfo.wrap(fibuf));
    } catch (Exception e) {
      LOG.error("Failed to read {}, size {}, offset {}: ", path, size, offset, e);
     return -ErrorCodes.EIO();
    }
  }

  public int getattrCallback(String path, ByteBuffer buf) {
    try {
      return getattr(path, FileStat.wrap(buf));
    } catch (Exception e) {
      LOG.error("Failed to getattr {}: ", path, e);
      return -ErrorCodes.EIO();
    }
  }

  public int readdirCallback(String path, long bufaddr, FuseFillDir filter, long offset,
      ByteBuffer fi) {
    try {
      return readdir(path, bufaddr, filter, offset, new FuseFileInfo(fi));
    } catch (Exception e) {
      LOG.error("Failed to readdir {}, offset {}: ", path, offset, e);
      return -ErrorCodes.EIO();
    }
  }

  public int unlinkCallback(String path) {
    try {
      return unlink(path);
    } catch (Exception e) {
      LOG.error("Failed to unlink {}: ", path, e);
      return -ErrorCodes.EIO();
    }
  }

  public int flushCallback(String path, ByteBuffer fi) {
    try {
      return flush(path, FuseFileInfo.wrap(fi));
    } catch (Exception e) {
      LOG.error("Failed to flush {}: ", path, e);
      return -ErrorCodes.EIO();
    }
  }

  public int releaseCallback(String path, ByteBuffer fi) {
    try {
      return release(path, FuseFileInfo.wrap(fi));
    } catch (Exception e) {
      LOG.error("Failed to release {}: ", path, e);
      return -ErrorCodes.EIO();
    }
  }

  public int chmodCallback(String path, long mode) {
    try {
      return chmod(path, mode);
    } catch (Exception e) {
      LOG.error("Failed to chmod {}, mode {}: ", path, mode, e);
      return -ErrorCodes.EIO();
    }
  }

  public int chownCallback(String path, long uid, long gid) {
    try {
      return chown(path, uid, gid);
    } catch (Exception e) {
      LOG.error("Failed to chown {}, uid {}, gid {}: ", path, uid, gid, e);
      return -ErrorCodes.EIO();
    }
  }

  public int createCallback(String path, long mode, ByteBuffer fi) {
    try {
      return create(path, mode, FuseFileInfo.wrap(fi));
    } catch (Exception e) {
      LOG.error("Failed to create {}, mode {}: ", path, mode, e);
      return -ErrorCodes.EIO();
    }
  }

  public int mkdirCallback(String path, long mode) {
    try {
      return mkdir(path, mode);
    } catch (Exception e) {
      LOG.error("Failed to mkdir {}, mode {}: ", path, mode, e);
      return -ErrorCodes.EIO();
    }
  }

  public int renameCallback(String oldPath, String newPath) {
    try {
      return rename(oldPath, newPath);
    } catch (Exception e) {
      LOG.error("Failed to rename {}, newPath {}: ", oldPath, newPath, e);
      return -ErrorCodes.EIO();
    }
  }

  public int rmdirCallback(String path) {
    try {
      return rmdir(path);
    } catch (Exception e) {
      LOG.error("Failed to rmdir {}: ", path, e);
      return -ErrorCodes.EIO();
    }
  }

  public int statfsCallback(String path, ByteBuffer stbuf) {
    try {
      return statfs(path, Statvfs.wrap(stbuf));
    } catch (Exception e) {
      LOG.error("Failed to statfs {}: ", path, e);
      return -ErrorCodes.EIO();
    }
  }

  public int truncateCallback(String path, long size) {
    try {
      return truncate(path, size);
    } catch (Exception e) {
      LOG.error("Failed to truncate {}, size {}: ", path, size, e);
      return -ErrorCodes.EIO();
    }
  }

  public int writeCallback(String path, ByteBuffer buf, long size, long offset, ByteBuffer fi) {
    try {
      return write(path, buf, size, offset, FuseFileInfo.wrap(fi));
    } catch (Exception e) {
      LOG.error("Failed to write {}, size {}, offset {}: ", path, size, offset, e);
      return -ErrorCodes.EIO();
    }
  }
}
