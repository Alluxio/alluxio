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

package alluxio.concurrent;

import alluxio.AlluxioURI;
import alluxio.SyncInfo;
import alluxio.collections.Pair;
import alluxio.concurrent.jsr.ForkJoinPool;
import alluxio.conf.AlluxioConfiguration;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsMode;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * Forwarder for {@link UnderFileSystem} objects that works through with ForkJoinPool's
 * managed blocking.
 *
 * If UFS calls are being done on a {@link alluxio.concurrent.jsr.ForkJoinWorkerThread}, then
 * this forwarder will make sure UFS operations are treated as blocking operations
 * for compensating the ForkJoinPool.
 *
 */
public class ManagedBlockingUfsForwarder implements UnderFileSystem {

  /** Underlying {@link UnderFileSystem}. **/
  private UnderFileSystem mUfs;

  /**
   * Creates {@link ManagedBlockingUfsForwarder} instance.
   *
   * @param ufs the underlying ufs
   */
  public ManagedBlockingUfsForwarder(UnderFileSystem ufs) {
    mUfs = ufs;
  }

  @Override
  public void cleanup() throws IOException {
    mUfs.cleanup();
  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {
    mUfs.connectFromMaster(hostname);
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
    mUfs.connectFromWorker(hostname);
  }

  @Override
  public OutputStream create(String path) throws IOException {
    return new ManagedBlockingUfsMethod<OutputStream>() {
      @Override
      public OutputStream execute() throws IOException {
        return mUfs.create(path);
      }
    }.get();
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    return new ManagedBlockingUfsMethod<OutputStream>() {
      @Override
      public OutputStream execute() throws IOException {
        return mUfs.create(path, options);
      }
    }.get();
  }

  @Override
  public OutputStream createNonexistingFile(String path) throws IOException {
    return new ManagedBlockingUfsMethod<OutputStream>() {
      @Override
      public OutputStream execute() throws IOException {
        return mUfs.createNonexistingFile(path);
      }
    }.get();
  }

  @Override
  public OutputStream createNonexistingFile(String path, CreateOptions options) throws IOException {
    return new ManagedBlockingUfsMethod<OutputStream>() {
      @Override
      public OutputStream execute() throws IOException {
        return mUfs.createNonexistingFile(path, options);
      }
    }.get();
  }

  @Override
  public boolean deleteDirectory(String path) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.deleteDirectory(path);
      }
    }.get();
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.deleteDirectory(path, options);
      }
    }.get();
  }

  @Override
  public boolean deleteExistingDirectory(String path) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.deleteExistingDirectory(path);
      }
    }.get();
  }

  @Override
  public boolean deleteExistingDirectory(String path, DeleteOptions options) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.deleteExistingDirectory(path, options);
      }
    }.get();
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.deleteFile(path);
      }
    }.get();
  }

  @Override
  public boolean deleteExistingFile(String path) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.deleteExistingFile(path);
      }
    }.get();
  }

  @Override
  public boolean exists(String path) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.exists(path);
      }
    }.get();
  }

  @Override
  public Pair<AccessControlList, DefaultAccessControlList> getAclPair(String path)
      throws IOException {
    return new ManagedBlockingUfsMethod<Pair<AccessControlList, DefaultAccessControlList>>() {
      @Override
      public Pair<AccessControlList, DefaultAccessControlList> execute() throws IOException {
        return mUfs.getAclPair(path);
      }
    }.get();
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return new ManagedBlockingUfsMethod<Long>() {
      @Override
      public Long execute() throws IOException {
        return mUfs.getBlockSizeByte(path);
      }
    }.get();
  }

  @Override
  public AlluxioConfiguration getConfiguration() throws IOException {
    return new ManagedBlockingUfsMethod<AlluxioConfiguration>() {
      @Override
      public AlluxioConfiguration execute() throws IOException {
        return mUfs.getConfiguration();
      }
    }.get();
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    return new ManagedBlockingUfsMethod<UfsDirectoryStatus>() {
      @Override
      public UfsDirectoryStatus execute() throws IOException {
        return mUfs.getDirectoryStatus(path);
      }
    }.get();
  }

  @Override
  public UfsDirectoryStatus getExistingDirectoryStatus(String path) throws IOException {
    return new ManagedBlockingUfsMethod<UfsDirectoryStatus>() {
      @Override
      public UfsDirectoryStatus execute() throws IOException {
        return mUfs.getExistingDirectoryStatus(path);
      }
    }.get();
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return new ManagedBlockingUfsMethod<List<String>>() {
      @Override
      public List<String> execute() throws IOException {
        return mUfs.getFileLocations(path);
      }
    }.get();
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    return new ManagedBlockingUfsMethod<List<String>>() {
      @Override
      public List<String> execute() throws IOException {
        return mUfs.getFileLocations(path, options);
      }
    }.get();
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    return new ManagedBlockingUfsMethod<UfsFileStatus>() {
      @Override
      public UfsFileStatus execute() throws IOException {
        return mUfs.getFileStatus(path);
      }
    }.get();
  }

  @Override
  public UfsFileStatus getExistingFileStatus(String path) throws IOException {
    return new ManagedBlockingUfsMethod<UfsFileStatus>() {
      @Override
      public UfsFileStatus execute() throws IOException {
        return mUfs.getExistingFileStatus(path);
      }
    }.get();
  }

  @Override
  public String getFingerprint(String path) {
    return mUfs.getFingerprint(path);
  }

  @Override
  public UfsMode getOperationMode(Map<String, UfsMode> physicalUfsState) {
    return mUfs.getOperationMode(physicalUfsState);
  }

  @Override
  public List<String> getPhysicalStores() {
    return mUfs.getPhysicalStores();
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return new ManagedBlockingUfsMethod<Long>() {
      @Override
      public Long execute() throws IOException {
        return mUfs.getSpace(path, type);
      }
    }.get();
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    return new ManagedBlockingUfsMethod<UfsStatus>() {
      @Override
      public UfsStatus execute() throws IOException {
        return mUfs.getStatus(path);
      }
    }.get();
  }

  @Override
  public UfsStatus getExistingStatus(String path) throws IOException {
    return new ManagedBlockingUfsMethod<UfsStatus>() {
      @Override
      public UfsStatus execute() throws IOException {
        return mUfs.getExistingStatus(path);
      }
    }.get();
  }

  @Override
  public String getUnderFSType() {
    return mUfs.getUnderFSType();
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.isDirectory(path);
      }
    }.get();
  }

  @Override
  public boolean isExistingDirectory(String path) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.isExistingDirectory(path);
      }
    }.get();
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.isFile(path);
      }
    }.get();
  }

  @Override
  public boolean isObjectStorage() {
    return mUfs.isObjectStorage();
  }

  @Override
  public boolean isSeekable() {
    return mUfs.isSeekable();
  }

  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
    return new ManagedBlockingUfsMethod<UfsStatus[]>() {
      @Override
      public UfsStatus[] execute() throws IOException {
        return mUfs.listStatus(path);
      }
    }.get();
  }

  @Override
  public UfsStatus[] listStatus(String path, ListOptions options) throws IOException {
    return new ManagedBlockingUfsMethod<UfsStatus[]>() {
      @Override
      public UfsStatus[] execute() throws IOException {
        return mUfs.listStatus(path, options);
      }
    }.get();
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.mkdirs(path);
      }
    }.get();
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.mkdirs(path, options);
      }
    }.get();
  }

  @Override
  public InputStream open(String path) throws IOException {
    return new ManagedBlockingUfsMethod<InputStream>() {
      @Override
      public InputStream execute() throws IOException {
        return mUfs.open(path);
      }
    }.get();
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    return new ManagedBlockingUfsMethod<InputStream>() {
      @Override
      public InputStream execute() throws IOException {
        return mUfs.open(path, options);
      }
    }.get();
  }

  @Override
  public InputStream openExistingFile(String path) throws IOException {
    return new ManagedBlockingUfsMethod<InputStream>() {
      @Override
      public InputStream execute() throws IOException {
        return mUfs.openExistingFile(path);
      }
    }.get();
  }

  @Override
  public InputStream openExistingFile(String path, OpenOptions options) throws IOException {
    return new ManagedBlockingUfsMethod<InputStream>() {
      @Override
      public InputStream execute() throws IOException {
        return mUfs.openExistingFile(path, options);
      }
    }.get();
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.renameDirectory(src, dst);
      }
    }.get();
  }

  @Override
  public boolean renameRenamableDirectory(String src, String dst) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.renameRenamableDirectory(src, dst);
      }
    }.get();
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.renameFile(src, dst);
      }
    }.get();
  }

  @Override
  public boolean renameRenamableFile(String src, String dst) throws IOException {
    return new ManagedBlockingUfsMethod<Boolean>() {
      @Override
      public Boolean execute() throws IOException {
        return mUfs.renameRenamableFile(src, dst);
      }
    }.get();
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return mUfs.resolveUri(ufsBaseUri, alluxioPath);
  }

  @Override
  public void setAclEntries(String path, List<AclEntry> aclEntries) throws IOException {
    new ManagedBlockingUfsMethod<Void>() {
      @Override
      public Void execute() throws IOException {
        mUfs.setAclEntries(path, aclEntries);
        return null;
      }
    }.get();
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    new ManagedBlockingUfsMethod<Void>() {
      @Override
      public Void execute() throws IOException {
        mUfs.setMode(path, mode);
        return null;
      }
    }.get();
  }

  @Override
  public void setOwner(String path, String owner, String group) throws IOException {
    new ManagedBlockingUfsMethod<Void>() {
      @Override
      public Void execute() throws IOException {
        mUfs.setOwner(path, owner, group);
        return null;
      }
    }.get();
  }

  @Override
  public boolean supportsFlush() throws IOException {
    return mUfs.supportsFlush();
  }

  @Override
  public boolean supportsActiveSync() {
    return mUfs.supportsActiveSync();
  }

  @Override
  public SyncInfo getActiveSyncInfo() throws IOException {
    return mUfs.getActiveSyncInfo();
  }

  @Override
  public void startSync(AlluxioURI uri) throws IOException {
    mUfs.startSync(uri);
  }

  @Override
  public void stopSync(AlluxioURI uri) throws IOException {
    mUfs.stopSync(uri);
  }

  @Override
  public boolean startActiveSyncPolling(long txId) throws IOException {
    return mUfs.startActiveSyncPolling(txId);
  }

  @Override
  public boolean stopActiveSyncPolling() throws IOException {
    return mUfs.stopActiveSyncPolling();
  }

  @Override
  public void close() throws IOException {
    mUfs.close();
  }

  /**
   * Utility class used to isolate calls into underlying UFS from concurrency compensation logic.
   * Note: This class used to make calls with a return value.
   *
   * @param <T> Return type of underlying UFS call
   */
  abstract class ManagedBlockingUfsMethod<T> implements ForkJoinPool.ManagedBlocker {
    private T mResult;
    private IOException mExc;

    /**
     * Implementation for the concrete UFS operation.
     *
     * @throws IOException
     */
    abstract T execute() throws IOException;

    /**
     * Executes the operation and returns the result.
     *
     * @return result the result from UFS call
     * @throws IOException
     */
    public T get() throws IOException {
      try {
        ForkJoinPoolHelper.safeManagedBlock(this);
        if (mExc != null) {
          throw mExc;
        }
        return mResult;
      } catch (InterruptedException exc) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted");
      }
    }

    @Override
    public boolean block() throws InterruptedException {
      try {
        mResult = execute();
      } catch (IOException exc) {
        mExc = exc;
      }
      return true;
    }

    @Override
    public boolean isReleasable() {
      return false;
    }
  }
}
