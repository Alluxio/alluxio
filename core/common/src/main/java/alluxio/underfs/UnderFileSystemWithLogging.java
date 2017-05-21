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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * This class forwards all calls to the {@link UnderFileSystem} interface to an internal
 * implementation. For methods which throw an {@link IOException}, it is implied that an
 * interaction with the underlying storage is possible. This class logs the enter/exit of all
 * such methods. Methods which do not throw exceptions will not be logged.
 */
public class UnderFileSystemWithLogging implements UnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(UnderFileSystemWithLogging.class);

  private final UnderFileSystem mUnderFileSystem;

  /**
   * Creates a new {@link UnderFileSystemWithLogging} which forwards all calls to the provided
   * {@link UnderFileSystem} implementation.
   *
   * @param ufs the implementation which will handle all the calls
   */
  // TODO(adit): Remove this method. ALLUXIO-2643.
  UnderFileSystemWithLogging(UnderFileSystem ufs) {
    mUnderFileSystem = ufs;
  }

  @Override
  public void close() throws IOException {
    call(new UfsCallable<Void>() {
      @Override
      public Void call() throws IOException {
        mUnderFileSystem.close();
        return null;
      }

      @Override
      public String toString() {
        return "Close";
      }
    });
  }

  @Override
  public void connectFromMaster(final String hostname) throws IOException {
    call(new UfsCallable<Void>() {
      @Override
      public Void call() throws IOException {
        mUnderFileSystem.connectFromMaster(hostname);
        return null;
      }

      @Override
      public String toString() {
        return String.format("ConnectFromMaster: hostname=%s", hostname);
      }
    });
  }

  @Override
  public void connectFromWorker(final String hostname) throws IOException {
    call(new UfsCallable<Void>() {
      @Override
      public Void call() throws IOException {
        mUnderFileSystem.connectFromWorker(hostname);
        return null;
      }

      @Override
      public String toString() {
        return String.format("ConnectFromWorker: hostname=%s", hostname);
      }
    });
  }

  @Override
  public OutputStream create(final String path) throws IOException {
    return call(new UfsCallable<OutputStream>() {
      @Override
      public OutputStream call() throws IOException {
        return mUnderFileSystem.create(path);
      }

      @Override
      public String toString() {
        return String.format("Create: path=%s", path);
      }
    });
  }

  @Override
  public OutputStream create(final String path, final CreateOptions options) throws IOException {
    return call(new UfsCallable<OutputStream>() {
      @Override
      public OutputStream call() throws IOException {
        return mUnderFileSystem.create(path, options);
      }

      @Override
      public String toString() {
        return String.format("Create: path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public boolean deleteDirectory(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.deleteDirectory(path);
      }

      @Override
      public String toString() {
        return String.format("DeleteDirectory: path=%s", path);
      }
    });
  }

  @Override
  public boolean deleteDirectory(final String path, final DeleteOptions options)
      throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.deleteDirectory(path, options);
      }

      @Override
      public String toString() {
        return String.format("DeleteDirectory: path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public boolean deleteFile(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.deleteFile(path);
      }

      @Override
      public String toString() {
        return String.format("DeleteFile: path=%s", path);
      }
    });
  }

  @Override
  public boolean exists(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.exists(path);
      }

      @Override
      public String toString() {
        return String.format("Exists: path=%s", path);
      }
    });
  }

  @Override
  public long getBlockSizeByte(final String path) throws IOException {
    return call(new UfsCallable<Long>() {
      @Override
      public Long call() throws IOException {
        return mUnderFileSystem.getBlockSizeByte(path);
      }

      @Override
      public String toString() {
        return String.format("GetBlockSizeByte: path=%s", path);
      }
    });
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(final String path) throws IOException {
    return call(new UfsCallable<UfsDirectoryStatus>() {
      @Override
      public UfsDirectoryStatus call() throws IOException {
        return mUnderFileSystem.getDirectoryStatus(path);
      }

      @Override
      public String toString() {
        return String.format("GetDirectoryStatus: path=%s", path);
      }
    });
  }

  @Override
  public List<String> getFileLocations(final String path) throws IOException {
    return call(new UfsCallable<List<String>>() {
      @Override
      public List<String> call() throws IOException {
        return mUnderFileSystem.getFileLocations(path);
      }

      @Override
      public String toString() {
        return String.format("GetFileLocations: path=%s", path);
      }
    });
  }

  @Override
  public List<String> getFileLocations(final String path, final FileLocationOptions options)
      throws IOException {
    return call(new UfsCallable<List<String>>() {
      @Override
      public List<String> call() throws IOException {
        return mUnderFileSystem.getFileLocations(path, options);
      }

      @Override
      public String toString() {
        return String.format("GetFileLocations: path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public UfsFileStatus getFileStatus(final String path) throws IOException {
    return call(new UfsCallable<UfsFileStatus>() {
      @Override
      public UfsFileStatus call() throws IOException {
        return mUnderFileSystem.getFileStatus(path);
      }

      @Override
      public String toString() {
        return String.format("GetFileStatus: path=%s", path);
      }
    });
  }

  @Override
  public long getSpace(final String path, final SpaceType type) throws IOException {
    return call(new UfsCallable<Long>() {
      @Override
      public Long call() throws IOException {
        return mUnderFileSystem.getSpace(path, type);
      }

      @Override
      public String toString() {
        return String.format("GetSpace: path=%s, type=%s", path, type);
      }
    });
  }

  @Override
  public String getUnderFSType() {
    return mUnderFileSystem.getUnderFSType();
  }

  @Override
  public boolean isDirectory(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.isDirectory(path);
      }

      @Override
      public String toString() {
        return String.format("IsDirectory: path=%s", path);
      }
    });
  }

  @Override
  public boolean isFile(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.isFile(path);
      }

      @Override
      public String toString() {
        return String.format("IsFile: path=%s", path);
      }
    });
  }

  @Override
  public UfsStatus[] listStatus(final String path) throws IOException {
    return call(new UfsCallable<UfsStatus[]>() {
      @Override
      public UfsStatus[] call() throws IOException {
        return mUnderFileSystem.listStatus(path);
      }

      @Override
      public String toString() {
        return String.format("ListStatus: path=%s", path);
      }
    });
  }

  @Override
  public UfsStatus[] listStatus(final String path, final ListOptions options)
      throws IOException {
    return call(new UfsCallable<UfsStatus[]>() {
      @Override
      public UfsStatus[] call() throws IOException {
        return mUnderFileSystem.listStatus(path, options);
      }

      @Override
      public String toString() {
        return String.format("ListStatus: path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public boolean mkdirs(final String path) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.mkdirs(path);
      }

      @Override
      public String toString() {
        return String.format("Mkdirs: path=%s", path);
      }
    });
  }

  @Override
  public boolean mkdirs(final String path, final MkdirsOptions options) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.mkdirs(path, options);
      }

      @Override
      public String toString() {
        return String.format("Mkdirs: path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public InputStream open(final String path) throws IOException {
    return call(new UfsCallable<InputStream>() {
      @Override
      public InputStream call() throws IOException {
        return mUnderFileSystem.open(path);
      }

      @Override
      public String toString() {
        return String.format("Open: path=%s", path);
      }
    });
  }

  @Override
  public InputStream open(final String path, final OpenOptions options) throws IOException {
    return call(new UfsCallable<InputStream>() {
      @Override
      public InputStream call() throws IOException {
        return mUnderFileSystem.open(path, options);
      }

      @Override
      public String toString() {
        return String.format("Open: path=%s, options=%s", path, options);
      }
    });
  }

  @Override
  public boolean renameDirectory(final String src, final String dst) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.renameDirectory(src, dst);
      }

      @Override
      public String toString() {
        return String.format("RenameDirectory: src=%s, dst=%s", src, dst);
      }
    });
  }

  @Override
  public boolean renameFile(final String src, final String dst) throws IOException {
    return call(new UfsCallable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        return mUnderFileSystem.renameFile(src, dst);
      }

      @Override
      public String toString() {
        return String.format("RenameFile: src=%s, dst=%s", src, dst);
      }
    });
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return mUnderFileSystem.resolveUri(ufsBaseUri, alluxioPath);
  }

  @Override
  public void setOwner(final String path, final String owner, final String group)
      throws IOException {
    call(new UfsCallable<Void>() {
      @Override
      public Void call() throws IOException {
        mUnderFileSystem.setOwner(path, owner, group);
        return null;
      }

      @Override
      public String toString() {
        return String.format("SetOwner: path=%s, owner=%s, group=%s", path, owner, group);
      }
    });
  }

  @Override
  public void setMode(final String path, final short mode) throws IOException {
    call(new UfsCallable<Void>() {
      @Override
      public Void call() throws IOException {
        mUnderFileSystem.setMode(path, mode);
        return null;
      }

      @Override
      public String toString() {
        return String.format("SetMode: path=%s, mode=%s", path, mode);
      }
    });
  }

  @Override
  public boolean supportsFlush() {
    return mUnderFileSystem.supportsFlush();
  }

  /**
   * This is only used in the test.
   *
   * @return the underlying {@link UnderFileSystem}
   */
  public UnderFileSystem getUnderFileSystem() {
    return mUnderFileSystem;
  }

  /**
   * Interface representing a callable to the under storage system which throws an
   * {@link IOException} if an error occurs during the external communication.
   *
   * @param <T> the return type of the callable
   */
  public interface UfsCallable<T> {
    /**
     * Executes the call.
     *
     * @return the result of the call
     */
    T call() throws IOException;
  }

  /**
   * A wrapper for invoking an {@link UfsCallable} with enter/exit point logging.
   *
   * @param callable the callable to invoke
   * @param <T> the return type
   * @return the result of the callable
   */
  private <T> T call(UfsCallable<T> callable) throws IOException {
    LOG.debug("Enter: {}", callable);
    try {
      T ret = callable.call();
      LOG.debug("Exit (OK): {}", callable);
      return ret;
    } catch (IOException e) {
      LOG.debug("Exit (Error): {}, Error={}", callable, e.getMessage());
      throw e;
    }
  }
}
