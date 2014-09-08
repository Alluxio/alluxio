package tachyon.web;

import tachyon.Constants;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;

import java.util.ArrayList;
import java.util.List;

public class UiFileInfo implements Comparable<UiFileInfo> {
  private final int mId;
  private final int mDependencyId;
  private final String mName;
  private final String mAbsolutePath;
  private final String mCheckpointPath;
  private final long mBlockSizeBytes;
  private final long mSize;
  private final long mCreationTimeMs;
  private final long mLastModificationTimeMs;
  private final boolean mInMemory;
  private final int mInMemoryPercent;
  private final boolean mIsDirectory;
  private final boolean mIsPinned;
  private List<String> mFileLocations;

  public UiFileInfo(ClientFileInfo fileInfo) {
    mId = fileInfo.getId();
    mDependencyId = fileInfo.getDependencyId();
    mName = fileInfo.getName();
    mAbsolutePath = fileInfo.getPath();
    mCheckpointPath = fileInfo.getUfsPath();
    mBlockSizeBytes = fileInfo.getBlockSizeByte();
    mSize = fileInfo.getLength();
    mCreationTimeMs = fileInfo.getCreationTimeMs();
    mLastModificationTimeMs = fileInfo.getLastModificationTimeMs();
    mInMemory = (100 == fileInfo.inMemoryPercentage);
    mInMemoryPercent = fileInfo.getInMemoryPercentage();
    mIsDirectory = fileInfo.isFolder;
    mIsPinned = fileInfo.isPinned;
    mFileLocations = new ArrayList<String>();
  }

  @Override
  public int compareTo(UiFileInfo o) {
    return mAbsolutePath.compareTo(o.getAbsolutePath());
  }

  public String getAbsolutePath() {
    return mAbsolutePath;
  }

  public String getBlockSizeBytes() {
    if (mIsDirectory) {
      return " ";
    } else {
      return CommonUtils.getSizeFromBytes(mBlockSizeBytes);
    }
  }

  public String getCheckpointPath() {
    return mCheckpointPath;
  }

  public String getCreationTime() {
    return CommonUtils.convertMsToDate(mCreationTimeMs);
  }

  public String getModificationTime() {
    return CommonUtils.convertMsToDate(mLastModificationTimeMs);
  }

  public int getDependencyId() {
    return mDependencyId;
  }

  public List<String> getFileLocations() {
    return mFileLocations;
  }

  public int getId() {
    return mId;
  }

  public boolean getInMemory() {
    return mInMemory;
  }

  public int getInMemoryPercentage() {
    return mInMemoryPercent;
  }

  public boolean getIsDirectory() {
    return mIsDirectory;
  }

  public boolean getNeedPin() {
    return mIsPinned;
  }

  public String getName() {
    if (Constants.PATH_SEPARATOR.equals(mAbsolutePath)) {
      return "root";
    } else {
      return mName;
    }
  }

  public String getSize() {
    if (mIsDirectory) {
      return " ";
    } else {
      return CommonUtils.getSizeFromBytes(mSize);
    }
  }

  public void setFileLocations(List<NetAddress> fileLocations) {
    for (NetAddress addr : fileLocations) {
      mFileLocations.add(addr.getMHost() + ":" + addr.getMPort());
    }
  }
}