package alluxio.underfs.sleepfs;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileStatus;
import alluxio.underfs.local.LocalUnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.CommonUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * An under file system for testing that sleeps a predefined amount of time before executing an
 * operation. The operation behavior will be equivalent to that of {@link LocalUnderFileSystem}.
 */
// TODO(calvin): Consider making the behavior customizable per method call
public class SleepingUnderFileSystem extends LocalUnderFileSystem {
  private final SleepingUnderFileSystemOptions mOptions;

  /**
   * Creates a new SleepingUnderFileSystem for the given uri.
   *
   * @param uri path belonging to this under file system
   */
  public SleepingUnderFileSystem(AlluxioURI uri, SleepingUnderFileSystemOptions options) {
    super(uri);
    mOptions = options;
  }

  @Override
  public void close() throws IOException {
    sleepIfNecessary(mOptions.getCloseMs());
    super.close();
  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {
    sleepIfNecessary(mOptions.getConnectFromMasterMs());
    super.connectFromMaster(hostname);
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
    sleepIfNecessary(mOptions.getConnectFromWorkerMs());
    super.connectFromWorker(hostname);
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    sleepIfNecessary(mOptions.getCreateMs());
    return super.create(path, options);
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    sleepIfNecessary(mOptions.getDeleteDirectoryMs());
    return super.deleteDirectory(path, options);
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    sleepIfNecessary(mOptions.getDeleteFileMs());
    return super.deleteFile(path);
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    sleepIfNecessary(mOptions.getGetBlockSizeByteMs());
    return super.getBlockSizeByte(path);
  }

  @Override
  public Object getConf() {
    sleepIfNecessary(mOptions.getGetConfMs());
    return super.getConf();
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    sleepIfNecessary(mOptions.getGetFileLocationsMs());
    return super.getFileLocations(path);
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options) throws IOException {
    sleepIfNecessary(mOptions.getGetFileLocationsMs());
    return super.getFileLocations(path, options);
  }

  @Override
  public long getFileSize(String path) throws IOException {
    sleepIfNecessary(mOptions.getGetFileSizeMs());
    return super.getFileSize(path);
  }

  @Override
  public String getGroup(String path) throws IOException {
    sleepIfNecessary(mOptions.getGetGroupMs());
    return super.getGroup(path);
  }

  @Override
  public short getMode(String path) throws IOException {
    sleepIfNecessary(mOptions.getGetModeMs());
    return super.getMode(path);
  }

  @Override
  public long getModificationTimeMs(String path) throws IOException {
    sleepIfNecessary(mOptions.getGetModificationTimeMs());
    return super.getModificationTimeMs(path);
  }

  @Override
  public String getOwner(String path) throws IOException {
    sleepIfNecessary(mOptions.getGetOwnerMs());
    return super.getOwner(path);
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    sleepIfNecessary(mOptions.getGetSpaceMs());
    return super.getSpace(path, type);
  }

  @Override
  public String getUnderFSType() {
    sleepIfNecessary(mOptions.getGetUnderFSTypeMs());
    return super.getUnderFSType();
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    sleepIfNecessary(mOptions.getIsDirectoryMs());
    return super.isDirectory(path);
  }

  @Override
  public boolean isFile(String path) throws IOException {
    sleepIfNecessary(mOptions.getIsFileMs());
    return super.isFile(path);
  }

  @Override
  public UnderFileStatus[] listStatus(String path) throws IOException {
    sleepIfNecessary(mOptions.getListStatusMs());
    return super.listStatus(path);
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    sleepIfNecessary(mOptions.getMkdirsMs());
    return super.mkdirs(path, options);
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    sleepIfNecessary(mOptions.getOpenMs());
    return super.open(path, options);
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    sleepIfNecessary(mOptions.getRenameDirectoryMs());
    return super.renameDirectory(src, dst);
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    sleepIfNecessary(mOptions.getRenameFileMs());
    return super.renameFile(src, dst);
  }

  @Override
  public void setConf(Object conf) {
    sleepIfNecessary(mOptions.getSetConfMs());
    super.setConf(conf);
  }

  @Override
  public void setOwner(String path, String owner, String group) throws IOException {
    sleepIfNecessary(mOptions.getSetOwnerMs());
    super.setOwner(path, owner, group);
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    sleepIfNecessary(mOptions.getSetModeMs());
    super.setMode(path, mode);
  }

  @Override
  public boolean supportsFlush() {
    sleepIfNecessary(mOptions.getSupportsFlushMs());
    return super.supportsFlush();
  }

  /**
   * Waits for the specified duration if the duration is non-negative. The thread will sleep if
   * the duration is 0.
   *
   * @param duration time to sleep, negative if the thread should not sleep
   */
  private void sleepIfNecessary(long duration) {
    if (duration >= 0) {
      CommonUtils.sleepMs(duration);
    }
  }
}
