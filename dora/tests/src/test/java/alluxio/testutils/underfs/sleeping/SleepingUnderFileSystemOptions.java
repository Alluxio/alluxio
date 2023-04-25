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

package alluxio.testutils.underfs.sleeping;

/**
 * Options for {@link SleepingUnderFileSystem} which will determine the amount of sleeping
 * injected into the various under file system calls. These options should be specified when
 * creating a {@link SleepingUnderFileSystemFactory}.
 */
public class SleepingUnderFileSystemOptions {
  private long mCleanupMs = -1;
  private long mCloseMs = -1;
  private long mConnectFromMasterMs = -1;
  private long mConnectFromWorkerMs = -1;
  private long mCreateMs = -1;
  private long mDeleteDirectoryMs = -1;
  private long mDeleteFileMs = -1;
  private long mExistsMs = -1;
  private long mGetBlockSizeByteMs = -1;
  private long mGetConfMs = -1;
  private long mGetDirectoryStatusMs = -1;
  private long mGetFileLocationsMs = -1;
  private long mGetFileStatusMs = -1;
  private long mGetFingerprintMs = -1;
  private long mGetSpaceMs = -1;
  private long mGetStatusMs = -1;
  private long mGetUnderFSTypeMs = -1;
  private long mIsDirectoryMs = -1;
  private long mIsFileMs = -1;
  private long mListStatusMs = -1;
  private long mListStatusWithOptionsMs = -1;
  private long mMkdirsMs = -1;
  private long mOpenMs = -1;
  private long mRenameDirectoryMs = -1;
  private long mRenameFileMs = -1;
  private long mRenameTemporaryFileMs = -1;
  private long mSetConfMs = -1;
  private long mSetOwnerMs = -1;
  private long mSetModeMs = -1;
  private long mSupportsFlushMs = -1;

  /**
   * @return milliseconds to sleep before executing a cleanup call
   */
  public long getCleanupMs() {
    return mCleanupMs;
  }

  /**
   * @param cleanupMs milliseconds to sleep before executing a cleanup call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setCleanupMs(long cleanupMs) {
    mCleanupMs = cleanupMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a close call
   */
  public long getCloseMs() {
    return mCloseMs;
  }

  /**
   * @param closeMs milliseconds to sleep before executing a close call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setCloseMs(long closeMs) {
    mCloseMs = closeMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a connect from master call
   */
  public long getConnectFromMasterMs() {
    return mConnectFromMasterMs;
  }

  /**
   * @param connectFromMasterMs milliseconds to sleep before executing a connect from master call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setConnectFromMasterMs(long connectFromMasterMs) {
    mConnectFromMasterMs = connectFromMasterMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a connect from worker call
   */
  public long getConnectFromWorkerMs() {
    return mConnectFromWorkerMs;
  }

  /**
   * @param connectFromWorkerMs milliseconds to sleep before executing a connect from worker call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setConnectFromWorkerMs(long connectFromWorkerMs) {
    mConnectFromWorkerMs = connectFromWorkerMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a create call
   */
  public long getCreateMs() {
    return mCreateMs;
  }

  /**
   * @param createMs milliseconds to sleep before executing a create call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setCreateMs(long createMs) {
    mCreateMs = createMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a delete directory call
   */
  public long getDeleteDirectoryMs() {
    return mDeleteDirectoryMs;
  }

  /**
   * @param deleteDirectoryMs milliseconds to sleep before executing a delete directory call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setDeleteDirectoryMs(long deleteDirectoryMs) {
    mDeleteDirectoryMs = deleteDirectoryMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a delete file call
   */
  public long getDeleteFileMs() {
    return mDeleteFileMs;
  }

  /**
   * @param deleteFileMs milliseconds to sleep before executing a delete file call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setDeleteFileMs(long deleteFileMs) {
    mDeleteFileMs = deleteFileMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing an exists call
   */
  public long getExistsMs() {
    return mExistsMs;
  }

  /**
   * @param existsMs milliseconds to sleep before executing an exists call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setExistsMs(long existsMs) {
    mExistsMs = existsMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a get block size call
   */
  public long getGetBlockSizeByteMs() {
    return mGetBlockSizeByteMs;
  }

  /**
   * @param getBlockSizeByteMs milliseconds to sleep before executing a get block size call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setGetBlockSizeByteMs(long getBlockSizeByteMs) {
    mGetBlockSizeByteMs = getBlockSizeByteMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a get conf call
   */
  public long getGetConfMs() {
    return mGetConfMs;
  }

  /**
   * @param getConfMs milliseconds to sleep before executing a get conf call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setGetConfMs(long getConfMs) {
    mGetConfMs = getConfMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a get directory status call
   */
  public long getGetDirectoryStatusMs() {
    return mGetDirectoryStatusMs;
  }

  /**
   * @param getDirectoryStatusMs milliseconds to sleep before executing a get directory status call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setGetDirectoryStatusMs(long getDirectoryStatusMs) {
    mGetDirectoryStatusMs = getDirectoryStatusMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a get file location call
   */
  public long getGetFileLocationsMs() {
    return mGetFileLocationsMs;
  }

  /**
   * @param getFileLocationsMs milliseconds to sleep before executing a get file location call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setGetFileLocationsMs(long getFileLocationsMs) {
    mGetFileLocationsMs = getFileLocationsMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a get file status call
   */
  public long getGetFileStatusMs() {
    return mGetFileStatusMs;
  }

  /**
   * @param getFileStatusMs milliseconds to sleep before executing a get file status call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setGetFileStatusMs(long getFileStatusMs) {
    mGetFileStatusMs = getFileStatusMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a getFingerprint call
   */
  public long getGetFingerprintMs() {
    return mGetFingerprintMs;
  }

  /**
   * @param getFingerprintMs milliseconds to sleep before executing a getFingerprint call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setGetFingerprintMs(long getFingerprintMs) {
    mGetFingerprintMs = getFingerprintMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a get space call
   */
  public long getGetSpaceMs() {
    return mGetSpaceMs;
  }

  /**
   * @param getStatusMs milliseconds to sleep before executing a getStatus call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setGetStatusMs(long getStatusMs) {
    mGetStatusMs = getStatusMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a getStatus call
   */
  public long getGetStatusMs() {
    return mGetStatusMs;
  }

  /**
   * @param getSpaceMs milliseconds to sleep before executing a get space call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setGetSpaceMs(long getSpaceMs) {
    mGetSpaceMs = getSpaceMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a get underfs type call
   */
  public long getGetUnderFSTypeMs() {
    return mGetUnderFSTypeMs;
  }

  /**
   * @param getUnderFSTypeMs milliseconds to sleep before executing a get underfs type call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setGetUnderFSTypeMs(long getUnderFSTypeMs) {
    mGetUnderFSTypeMs = getUnderFSTypeMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a is directory call
   */
  public long getIsDirectoryMs() {
    return mIsDirectoryMs;
  }

  /**
   * @param isDirectoryMs milliseconds to sleep before executing a is directory call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setIsDirectoryMs(long isDirectoryMs) {
    mIsDirectoryMs = isDirectoryMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a is file call
   */
  public long getIsFileMs() {
    return mIsFileMs;
  }

  /**
   * @param isFileMs milliseconds to sleep before executing a is file call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setIsFileMs(long isFileMs) {
    mIsFileMs = isFileMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a list status call
   */
  public long getListStatusMs() {
    return mListStatusMs;
  }

  /**
   * @param listStatusMs milliseconds to sleep before executing a list status call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setListStatusMs(long listStatusMs) {
    mListStatusMs = listStatusMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a list status with options call
   */
  public long getListStatusWithOptionsMs() {
    return mListStatusWithOptionsMs;
  }

  /**
   * @param listStatusWithOptionsMs milliseconds to sleep before executing a list status
   *                                with options call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setListStatusWithOptionsMs(long listStatusWithOptionsMs) {
    mListStatusWithOptionsMs = listStatusWithOptionsMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a mkdirs call
   */
  public long getMkdirsMs() {
    return mMkdirsMs;
  }

  /**
   * @param mkdirsMs milliseconds to sleep before executing a mkdirs call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setMkdirsMs(long mkdirsMs) {
    mMkdirsMs = mkdirsMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing an open call
   */
  public long getOpenMs() {
    return mOpenMs;
  }

  /**
   * @param openMs milliseconds to sleep before executing an open call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setOpenMs(long openMs) {
    mOpenMs = openMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a rename directory call
   */
  public long getRenameDirectoryMs() {
    return mRenameDirectoryMs;
  }

  /**
   * @param renameDirectoryMs milliseconds to sleep before executing a rename directory call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setRenameDirectoryMs(long renameDirectoryMs) {
    mRenameDirectoryMs = renameDirectoryMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a rename file call on a non-temporary file
   */
  public long getRenameFileMs() {
    return mRenameFileMs;
  }

  /**
   * @param renameFileMs milliseconds to sleep before executing a rename file call on a
   *                     non-temporary file
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setRenameFileMs(long renameFileMs) {
    mRenameFileMs = renameFileMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a rename file call on a temporary file
   */
  public long getRenameTemporaryFileMs() {
    return mRenameTemporaryFileMs;
  }

  /**
   * @param renameTemporaryFileMs milliseconds to sleep before executing a rename file call on a
   *                              temporary file
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setRenameTemporaryFileMs(long renameTemporaryFileMs) {
    mRenameTemporaryFileMs = renameTemporaryFileMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a set conf call
   */
  public long getSetConfMs() {
    return mSetConfMs;
  }

  /**
   * @param setConfMs milliseconds to sleep before executing a set conf call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setSetConfMs(long setConfMs) {
    mSetConfMs = setConfMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a set owner call
   */
  public long getSetOwnerMs() {
    return mSetOwnerMs;
  }

  /**
   * @param setOwnerMs milliseconds to sleep before executing a set owner call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setSetOwnerMs(long setOwnerMs) {
    mSetOwnerMs = setOwnerMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a set mode call
   */
  public long getSetModeMs() {
    return mSetModeMs;
  }

  /**
   * @param setModeMs milliseconds to sleep before executing a set mode call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setSetModeMs(long setModeMs) {
    mSetModeMs = setModeMs;
    return this;
  }

  /**
   * @return milliseconds to sleep before executing a supports flush call
   */
  public long getSupportsFlushMs() {
    return mSupportsFlushMs;
  }

  /**
   * @param supportsFlushMs milliseconds to sleep before executing a supports flush call
   * @return the updated object
   */
  public SleepingUnderFileSystemOptions setSupportsFlushMs(long supportsFlushMs) {
    mSupportsFlushMs = supportsFlushMs;
    return this;
  }
}
