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

package alluxio.fuse;

import alluxio.jnifuse.AbstractFuseFileSystem;
import alluxio.jnifuse.ErrorCodes;
import alluxio.jnifuse.FuseFillDir;
import alluxio.jnifuse.struct.FileStat;
import alluxio.jnifuse.struct.FuseFileInfo;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

/**
 * Stack FS implements the FUSE callbacks defined by jni-fuse
 * without interactions with Alluxio clients/servers.
 * <p>
 * Stack FS mounts a local filesystem path to another local filesystem path.
 * All the operations target the Stack FS mount point will be directly
 * trigger on the local filesystem mounted path without complex added logics.
 * </p>
 * <p>
 * This class is mainly added for testing purposes to understand the
 * performance overhead introduced by jni-fuse and provides an upper-bound
 * performance data for Alluxio jni-fuse implementations.
 * </p>
 */
public class StackFS extends AbstractFuseFileSystem {
  private final Path mRoot;

  /**
   * @param root root
   * @param mountPoint mount point
   */
  public StackFS(Path root, Path mountPoint) {
    super(mountPoint);
    mRoot = root;
  }

  private String transformPath(String path) {
    return mRoot + path;
  }

  private int getMode(Path path) {
    int mode = 0;
    if (Files.isDirectory(path)) {
      mode |= FileStat.S_IFDIR;
    } else {
      mode |= FileStat.S_IFREG;
    }
    if (Files.isReadable(path)) {
      mode |= FileStat.ALL_READ;
    }
    if (Files.isExecutable(path)) {
      mode |= FileStat.S_IXUGO;
    }
    return mode;
  }

  @Override
  public int getattr(String path, FileStat stat) {
    path = transformPath(path);
    try {
      Path filePath = Paths.get(path);
      if (!Files.exists(filePath)) {
        return -ErrorCodes.ENOENT();
      }
      BasicFileAttributes attributes = Files.readAttributes(filePath, BasicFileAttributes.class);

      stat.st_size.set(attributes.size());
      stat.st_blksize.set((int) Math.ceil((double) attributes.size() / 512));

      stat.st_ctim.tv_sec.set(attributes.creationTime().to(TimeUnit.SECONDS));
      stat.st_ctim.tv_nsec.set(attributes.creationTime().to(TimeUnit.NANOSECONDS));
      stat.st_mtim.tv_sec.set(attributes.lastModifiedTime().to(TimeUnit.SECONDS));
      stat.st_mtim.tv_nsec.set(attributes.lastModifiedTime().to(TimeUnit.NANOSECONDS));

      int uid = (Integer) Files.getAttribute(filePath, "unix:uid");
      int gid = (Integer) Files.getAttribute(filePath, "unix:gid");
      stat.st_uid.set(uid);
      stat.st_gid.set(gid);

      int mode = getMode(filePath);
      stat.st_mode.set(mode);
    } catch (Exception e) {
      e.printStackTrace();
      return -ErrorCodes.EIO();
    }
    return 0;
  }

  @Override
  public int readdir(String path, long bufaddr, FuseFillDir filter, long offset, FuseFileInfo fi) {
    path = transformPath(path);
    File dir = new File(path);
    filter.apply(bufaddr, ".", null, 0);
    filter.apply(bufaddr, "..", null, 0);
    File[] subfiles = dir.listFiles();
    if (subfiles != null) {
      for (File subfile : subfiles) {
        filter.apply(bufaddr, subfile.getName(), null, 0);
      }
    }
    return 0;
  }

  @Override
  public int open(String path, FuseFileInfo fi) {
    path = transformPath(path);
    try (FileInputStream fis = new FileInputStream(path)) {
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      return -ErrorCodes.EIO();
    }
  }

  @Override
  public int read(String path, ByteBuffer buf, long size, long offset, FuseFileInfo fi) {
    path = transformPath(path);
    int nread = 0;
    try (FileInputStream fis = new FileInputStream(path)) {
      byte[] tmpbuf = new byte[(int) size];
      long nskipped = fis.skip(offset);
      nread = fis.read(tmpbuf, 0, (int) size);
      buf.put(tmpbuf, 0, nread);
    } catch (IndexOutOfBoundsException e) {
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      return -ErrorCodes.EIO();
    }
    return nread;
  }

  @Override
  public int flush(String path, FuseFileInfo fi) {
    return 0;
  }

  @Override
  public int release(String path, FuseFileInfo fi) {
    return 0;
  }

  @Override
  public String getFileSystemName() {
    return "jnifuse-stackfs";
  }
}
