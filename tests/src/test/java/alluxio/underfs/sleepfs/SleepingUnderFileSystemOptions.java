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

  public void setCloseMs(long closeMs) {
    mCloseMs = closeMs;
  }

  public long getConnectFromMasterMs() {
    return mConnectFromMasterMs;
  }

  public void setConnectFromMasterMs(long connectFromMasterMs) {
    mConnectFromMasterMs = connectFromMasterMs;
  }

  public long getConnectFromWorkerMs() {
    return mConnectFromWorkerMs;
  }

  public void setConnectFromWorkerMs(long connectFromWorkerMs) {
    mConnectFromWorkerMs = connectFromWorkerMs;
  }

  public long getCreateMs() {
    return mCreateMs;
  }

  public void setCreateMs(long createMs) {
    mCreateMs = createMs;
  }

  public long getDeleteDirectoryMs() {
    return mDeleteDirectoryMs;
  }

  public void setDeleteDirectoryMs(long deleteDirectoryMs) {
    mDeleteDirectoryMs = deleteDirectoryMs;
  }

  public long getDeleteFileMs() {
    return mDeleteFileMs;
  }

  public void setDeleteFileMs(long deleteFileMs) {
    mDeleteFileMs = deleteFileMs;
  }

  public long getGetBlockSizeByteMs() {
    return mGetBlockSizeByteMs;
  }

  public void setGetBlockSizeByteMs(long getBlockSizeByteMs) {
    mGetBlockSizeByteMs = getBlockSizeByteMs;
  }

  public long getGetConfMs() {
    return mGetConfMs;
  }

  public void setGetConfMs(long getConfMs) {
    mGetConfMs = getConfMs;
  }

  public long getGetFileLocationsMs() {
    return mGetFileLocationsMs;
  }

  public void setGetFileLocationsMs(long getFileLocationsMs) {
    mGetFileLocationsMs = getFileLocationsMs;
  }

  public long getGetFileSizeMs() {
    return mGetFileSizeMs;
  }

  public void setGetFileSizeMs(long getFileSizeMs) {
    mGetFileSizeMs = getFileSizeMs;
  }

  public long getGetGroupMs() {
    return mGetGroupMs;
  }

  public void setGetGroupMs(long getGroupMs) {
    mGetGroupMs = getGroupMs;
  }

  public long getGetModeMs() {
    return mGetModeMs;
  }

  public void setGetModeMs(long getModeMs) {
    mGetModeMs = getModeMs;
  }

  public long getGetModificationTimeMs() {
    return mGetModificationTimeMs;
  }

  public void setGetModificationTimeMs(long getModificationTimeMs) {
    mGetModificationTimeMs = getModificationTimeMs;
  }

  public long getGetOwnerMs() {
    return mGetOwnerMs;
  }

  public void setGetOwnerMs(long getOwnerMs) {
    mGetOwnerMs = getOwnerMs;
  }

  public long getGetSpaceMs() {
    return mGetSpaceMs;
  }

  public void setGetSpaceMs(long getSpaceMs) {
    mGetSpaceMs = getSpaceMs;
  }

  public long getGetUnderFSTypeMs() {
    return mGetUnderFSTypeMs;
  }

  public void setGetUnderFSTypeMs(long getUnderFSTypeMs) {
    mGetUnderFSTypeMs = getUnderFSTypeMs;
  }

  public long getIsDirectoryMs() {
    return mIsDirectoryMs;
  }

  public void setIsDirectoryMs(long isDirectoryMs) {
    mIsDirectoryMs = isDirectoryMs;
  }

  public long getIsFileMs() {
    return mIsFileMs;
  }

  public void setIsFileMs(long isFileMs) {
    mIsFileMs = isFileMs;
  }

  public long getListStatusMs() {
    return mListStatusMs;
  }

  public void setListStatusMs(long listStatusMs) {
    mListStatusMs = listStatusMs;
  }

  public long getMkdirsMs() {
    return mMkdirsMs;
  }

  public void setMkdirsMs(long mkdirsMs) {
    mMkdirsMs = mkdirsMs;
  }

  public long getOpenMs() {
    return mOpenMs;
  }

  public void setOpenMs(long openMs) {
    mOpenMs = openMs;
  }

  public long getRenameDirectoryMs() {
    return mRenameDirectoryMs;
  }

  public void setRenameDirectoryMs(long renameDirectoryMs) {
    mRenameDirectoryMs = renameDirectoryMs;
  }

  public long getRenameFileMs() {
    return mRenameFileMs;
  }

  public void setRenameFileMs(long renameFileMs) {
    mRenameFileMs = renameFileMs;
  }

  public long getRenameTemporaryFileMs() {
    return mRenameTemporaryFileMs;
  }

  public void setRenameTemporaryFileMs(long renameTemporaryFileMs) {
    mRenameTemporaryFileMs = renameTemporaryFileMs;
  }

  public long getSetConfMs() {
    return mSetConfMs;
  }

  public void setSetConfMs(long setConfMs) {
    mSetConfMs = setConfMs;
  }

  public long getSetOwnerMs() {
    return mSetOwnerMs;
  }

  public void setSetOwnerMs(long setOwnerMs) {
    mSetOwnerMs = setOwnerMs;
  }

  public long getSetModeMs() {
    return mSetModeMs;
  }

  public void setSetModeMs(long setModeMs) {
    mSetModeMs = setModeMs;
  }

  public long getSupportsFlushMs() {
    return mSupportsFlushMs;
  }

  public void setSupportsFlushMs(long supportsFlushMs) {
    mSupportsFlushMs = supportsFlushMs;
  }
}
