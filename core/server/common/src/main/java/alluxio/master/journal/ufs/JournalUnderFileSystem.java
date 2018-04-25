package alluxio.master.journal.ufs;

import alluxio.AlluxioURI;
import alluxio.exception.JournalClosedException;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wrapper around UnderFileSystem which can be switched between read-only and read-write modes.
 *
 * This is useful for guaranteeing that secondary masters cannot accidentally write to the ufs
 * journal.
 */
public class JournalUnderFileSystem implements UnderFileSystem {
  private final UnderFileSystem mUfs;
  private final AtomicBoolean mWriteMode;

  /**
   * @param ufs the ufs to delegate to
   */
  public JournalUnderFileSystem(UnderFileSystem ufs) {
    mUfs = ufs;
    mWriteMode = new AtomicBoolean(false);
  }

  /**
   * Sets this UFS instance to be either read-only or writable.
   *
   * @param writable whether to set the UFS to writable or read-only
   */
  public void setWritable(boolean writable) {
    mWriteMode.set(writable);
  }

  private void checkWriteMode() throws IOException {
    if (!mWriteMode.get()) {
      throw new JournalClosedException("Cannot write to journal. Journal is in read-only mode.")
          .toIOException();
    }
  }

  /// Methods that require write access

  @Override
  public OutputStream create(String path) throws IOException {
    checkWriteMode();
    return mUfs.create(path);
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    checkWriteMode();
    return mUfs.create(path, options);
  }

  @Override
  public boolean deleteDirectory(String path) throws IOException {
    checkWriteMode();
    return mUfs.deleteDirectory(path);
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    checkWriteMode();
    return mUfs.deleteDirectory(path, options);
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    checkWriteMode();
    return mUfs.deleteFile(path);
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    checkWriteMode();
    return mUfs.mkdirs(path);
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    checkWriteMode();
    return mUfs.mkdirs(path, options);
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    checkWriteMode();
    return mUfs.renameDirectory(src, dst);
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    checkWriteMode();
    return mUfs.renameFile(src, dst);
  }

  /// Methods that require read access

  @Override
  public void connectFromMaster(String hostname) throws IOException {
    mUfs.connectFromMaster(hostname);
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
    mUfs.connectFromWorker(hostname);
  }

  @Override
  public boolean exists(String path) throws IOException {
    return mUfs.exists(path);
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return mUfs.getBlockSizeByte(path);
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    return mUfs.getDirectoryStatus(path);
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return mUfs.getFileLocations(path);
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options) throws IOException {
    return mUfs.getFileLocations(path, options);
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    return mUfs.getFileStatus(path);
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
    return mUfs.getSpace(path, type);
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    return mUfs.getStatus(path);
  }

  @Override
  public String getUnderFSType() {
    return mUfs.getUnderFSType();
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    return mUfs.isDirectory(path);
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return mUfs.isFile(path);
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
    return mUfs.listStatus(path);
  }

  @Override
  public UfsStatus[] listStatus(String path, ListOptions options) throws IOException {
    return mUfs.listStatus(path, options);
  }

  @Override
  public InputStream open(String path) throws IOException {
    return mUfs.open(path);
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    return mUfs.open(path, options);
  }

  @Override
  public AlluxioURI resolveUri(AlluxioURI ufsBaseUri, String alluxioPath) {
    return mUfs.resolveUri(ufsBaseUri, alluxioPath);
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    mUfs.setMode(path, mode);
  }

  @Override
  public void setOwner(String path, String owner, String group) throws IOException {
    mUfs.setOwner(path, owner, group);
  }

  @Override
  public boolean supportsFlush() {
    return mUfs.supportsFlush();
  }

  @Override
  public void close() throws IOException {
    mUfs.close();
  }
}
