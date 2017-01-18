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

package alluxio.underfs.sleepfs;

/**
 * Options for {@link SleepingUnderFileSystem} which will determine the amount of sleeping
 * injected into the various under file system calls. These options should be specified when
 * creating a {@link SleepingUnderFileSystemFactory}.
 */
public class SleepingUnderFileSystemOptions {
  private long mCloseMs = -1;
  private long mConnectFromMasterMs = -1;
  private long mConnectFromWorkerMs = -1;
  private long mCreateMs = -1;
  private long mDeleteDirectoryMs = -1;
  private long mDeleteFileMs = -1;
  private long mGetBlockSizeByteMs = -1;
  private long mGetConfMs = -1;
  private long mGetFileLocationsMs = -1;
  private long mGetFileSizeMs = -1;
  private long mGetGroupMs = -1;
  private long mGetModeMs = -1;
  private long mGetModificationTimeMs = -1;
  private long mGetOwnerMs = -1;
  private long mGetSpaceMs = -1;
  private long mGetUnderFSTypeMs = -1;
  private long mIsDirectoryMs = -1;
  private long mIsFileMs = -1;
  private long mListStatusMs = -1;
  private long mMkdirsMs = -1;
  private long mOpenMs = -1;
  private long mRenameDirectoryMs = -1;
  private long mRenameFileMs = -1;
  private long mRenameTemporaryFileMs = -1;
  private long mSetConfMs = -1;
  private long mSetOwnerMs = -1;
  private long mSetModeMs = -1;
  private long mSupportsFlushMs = -1;

  public long getCloseMs() {
    return mCloseMs;
  }

  public SleepingUnderFileSystemOptions setCloseMs(long closeMs) {
    mCloseMs = closeMs;
    return this;
  }

  public long getConnectFromMasterMs() {
    return mConnectFromMasterMs;
  }

  public SleepingUnderFileSystemOptions setConnectFromMasterMs(long connectFromMasterMs) {
    mConnectFromMasterMs = connectFromMasterMs;
    return this;
  }

  public long getConnectFromWorkerMs() {
    return mConnectFromWorkerMs;
  }

  public SleepingUnderFileSystemOptions setConnectFromWorkerMs(long connectFromWorkerMs) {
    mConnectFromWorkerMs = connectFromWorkerMs;
    return this;
  }

  public long getCreateMs() {
    return mCreateMs;
  }

  public SleepingUnderFileSystemOptions setCreateMs(long createMs) {
    mCreateMs = createMs;
    return this;
  }

  public long getDeleteDirectoryMs() {
    return mDeleteDirectoryMs;
  }

  public SleepingUnderFileSystemOptions setDeleteDirectoryMs(long deleteDirectoryMs) {
    mDeleteDirectoryMs = deleteDirectoryMs;
    return this;
  }

  public long getDeleteFileMs() {
    return mDeleteFileMs;
  }

  public SleepingUnderFileSystemOptions setDeleteFileMs(long deleteFileMs) {
    mDeleteFileMs = deleteFileMs;
    return this;
  }

  public long getGetBlockSizeByteMs() {
    return mGetBlockSizeByteMs;
  }

  public SleepingUnderFileSystemOptions setGetBlockSizeByteMs(long getBlockSizeByteMs) {
    mGetBlockSizeByteMs = getBlockSizeByteMs;
    return this;
  }

  public long getGetConfMs() {
    return mGetConfMs;
  }

  public SleepingUnderFileSystemOptions setGetConfMs(long getConfMs) {
    mGetConfMs = getConfMs;
    return this;
  }

  public long getGetFileLocationsMs() {
    return mGetFileLocationsMs;
  }

  public SleepingUnderFileSystemOptions setGetFileLocationsMs(long getFileLocationsMs) {
    mGetFileLocationsMs = getFileLocationsMs;
    return this;
  }

  public long getGetFileSizeMs() {
    return mGetFileSizeMs;
  }

  public SleepingUnderFileSystemOptions setGetFileSizeMs(long getFileSizeMs) {
    mGetFileSizeMs = getFileSizeMs;
    return this;
  }

  public long getGetGroupMs() {
    return mGetGroupMs;
  }

  public SleepingUnderFileSystemOptions setGetGroupMs(long getGroupMs) {
    mGetGroupMs = getGroupMs;
    return this;
  }

  public long getGetModeMs() {
    return mGetModeMs;
  }

  public SleepingUnderFileSystemOptions setGetModeMs(long getModeMs) {
    mGetModeMs = getModeMs;
    return this;
  }

  public long getGetModificationTimeMs() {
    return mGetModificationTimeMs;
  }

  public SleepingUnderFileSystemOptions setGetModificationTimeMs(long getModificationTimeMs) {
    mGetModificationTimeMs = getModificationTimeMs;
    return this;
  }

  public long getGetOwnerMs() {
    return mGetOwnerMs;
  }

  public SleepingUnderFileSystemOptions setGetOwnerMs(long getOwnerMs) {
    mGetOwnerMs = getOwnerMs;
    return this;
  }

  public long getGetSpaceMs() {
    return mGetSpaceMs;
  }

  public SleepingUnderFileSystemOptions setGetSpaceMs(long getSpaceMs) {
    mGetSpaceMs = getSpaceMs;
    return this;
  }

  public long getGetUnderFSTypeMs() {
    return mGetUnderFSTypeMs;
  }

  public SleepingUnderFileSystemOptions setGetUnderFSTypeMs(long getUnderFSTypeMs) {
    mGetUnderFSTypeMs = getUnderFSTypeMs;
    return this;
  }

  public long getIsDirectoryMs() {
    return mIsDirectoryMs;
  }

  public SleepingUnderFileSystemOptions setIsDirectoryMs(long isDirectoryMs) {
    mIsDirectoryMs = isDirectoryMs;
    return this;
  }

  public long getIsFileMs() {
    return mIsFileMs;
  }

  public SleepingUnderFileSystemOptions setIsFileMs(long isFileMs) {
    mIsFileMs = isFileMs;
    return this;
  }

  public long getListStatusMs() {
    return mListStatusMs;
  }

  public SleepingUnderFileSystemOptions setListStatusMs(long listStatusMs) {
    mListStatusMs = listStatusMs;
    return this;
  }

  public long getMkdirsMs() {
    return mMkdirsMs;
  }

  public SleepingUnderFileSystemOptions setMkdirsMs(long mkdirsMs) {
    mMkdirsMs = mkdirsMs;
    return this;
  }

  public long getOpenMs() {
    return mOpenMs;
  }

  public SleepingUnderFileSystemOptions setOpenMs(long openMs) {
    mOpenMs = openMs;
    return this;
  }

  public long getRenameDirectoryMs() {
    return mRenameDirectoryMs;
  }

  public SleepingUnderFileSystemOptions setRenameDirectoryMs(long renameDirectoryMs) {
    mRenameDirectoryMs = renameDirectoryMs;
    return this;
  }

  public long getRenameFileMs() {
    return mRenameFileMs;
  }

  public SleepingUnderFileSystemOptions setRenameFileMs(long renameFileMs) {
    mRenameFileMs = renameFileMs;
    return this;
  }

  public long getRenameTemporaryFileMs() {
    return mRenameTemporaryFileMs;
  }

  public SleepingUnderFileSystemOptions setRenameTemporaryFileMs(long renameTemporaryFileMs) {
    mRenameTemporaryFileMs = renameTemporaryFileMs;
    return this;
  }

  public long getSetConfMs() {
    return mSetConfMs;
  }

  public SleepingUnderFileSystemOptions setSetConfMs(long setConfMs) {
    mSetConfMs = setConfMs;
    return this;
  }

  public long getSetOwnerMs() {
    return mSetOwnerMs;
  }

  public SleepingUnderFileSystemOptions setSetOwnerMs(long setOwnerMs) {
    mSetOwnerMs = setOwnerMs;
    return this;
  }

  public long getSetModeMs() {
    return mSetModeMs;
  }

  public SleepingUnderFileSystemOptions setSetModeMs(long setModeMs) {
    mSetModeMs = setModeMs;
    return this;
  }

  public long getSupportsFlushMs() {
    return mSupportsFlushMs;
  }

  public SleepingUnderFileSystemOptions setSupportsFlushMs(long supportsFlushMs) {
    mSupportsFlushMs = supportsFlushMs;
    return this;
  }
}
