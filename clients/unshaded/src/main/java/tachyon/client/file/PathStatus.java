package tachyon.client.file;

import tachyon.thrift.FileInfo;

import java.util.List;

/**
 * Wrapper around {@link FileInfo}. Represents the metadata about a file or folder in Tachyon.
 */
public class PathStatus {
  private final FileInfo mInfo;

  public PathStatus(FileInfo info) {
    mInfo = info;
  }

  public List<Long> getBlockIds() {
    return mInfo.getBlockIds();
  }

  public long getBlockSizeBytes() {
    return mInfo.getBlockSizeBytes();
  }

  public long getCreationTimeMs() {
    return mInfo.getCreationTimeMs();
  }

  public long getFileId() {
    return mInfo.getFileId();
  }

  public String getGroupName() {
    return mInfo.getGroupName();
  }

  public int getInMemoryPercentage() {
    return mInfo.getInMemoryPercentage();
  }

  public long getLastModificationTimeMs() {
    return mInfo.getLastModificationTimeMs();
  }

  public long getLength() {
    return mInfo.getLength();
  }

  public String getName() {
    return mInfo.getName();
  }

  public String getPath() {
    return mInfo.getPath();
  }

  public int getPermission() {
    return mInfo.getPermission();
  }

  public long getTtl() {
    return mInfo.getTtl();
  }

  public String getUfsPath() {
    return mInfo.getUfsPath();
  }

  public String getUsername() {
    return mInfo.getUserName();
  }
}
