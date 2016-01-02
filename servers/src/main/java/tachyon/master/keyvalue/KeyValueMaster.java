/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.keyvalue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.master.MasterBase;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.journal.Journal;
import tachyon.master.journal.JournalOutputStream;
import tachyon.proto.journal.Journal.JournalEntry;
import tachyon.thrift.KeyValueMasterClientService;
import tachyon.thrift.PartitionInfo;
import tachyon.util.IdUtils;
import tachyon.util.io.PathUtils;

/**
 * The key-value master stores key-value store information in Tachyon, including the partitions of
 * each key-value store.
 */
public final class KeyValueMaster extends MasterBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final FileSystemMaster mFileSystemMaster;

  /** Map from file id of a complete store to the list of partitions in this store. */
  private final Map<Long, List<PartitionInfo>> mCompleteStoreToPartitions;
  /**
   * Map from file id of an incomplete store (i.e., some one is still writing new partitions) to the
   * list of partitions in this store.
   */
  private final Map<Long, List<PartitionInfo>> mIncompleteStoreToPartitions;
  private final Object mLock = new Object();

  /**
   * @param baseDirectory the base journal directory
   * @return the journal directory for this master
   */
  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, Constants.KEY_VALUE_MASTER_NAME);
  }

  /**
   * @param fileSystemMaster handler to a {@link FileSystemMaster} instance
   * @param journal the journal
   */
  public KeyValueMaster(FileSystemMaster fileSystemMaster, Journal journal) {
    super(journal, 2);
    mFileSystemMaster = fileSystemMaster;
    mCompleteStoreToPartitions = Maps.newHashMap();
    mIncompleteStoreToPartitions = Maps.newHashMap();
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<String, TProcessor>();
    services.put(Constants.KEY_VALUE_MASTER_CLIENT_SERVICE_NAME,
        new KeyValueMasterClientService.Processor<KeyValueMasterClientServiceHandler>(
            new KeyValueMasterClientServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.KEY_VALUE_MASTER_NAME;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    // TODO(binfan): process journal
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    // TODO(binfan): output journal
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    super.start(isLeader);
  }

  /**
   * Marks a partition complete and adds it to an incomplete key-value store.
   *
   * @param path URI of the key-value store
   * @param info information of this completed parition
   * @throws FileDoesNotExistException if the key-value store URI does not exists
   */
  public void completePartition(TachyonURI path, PartitionInfo info)
      throws FileDoesNotExistException {
    final long fileId = mFileSystemMaster.getFileId(path);
    if (fileId == IdUtils.INVALID_FILE_ID) {
      throw new FileDoesNotExistException(
          String.format("Failed to completePartition: path %s does not exist", path));
    }

    synchronized (mLock) {
      if (!mIncompleteStoreToPartitions.containsKey(fileId)) {
        // TODO(binfan): throw a better exception
        throw new FileDoesNotExistException("fill me");
      }
      mIncompleteStoreToPartitions.get(fileId).add(info);
    }
  }

  /**
   * Marks a key-value store complete.
   *
   * @param path URI of the key-value store
   * @throws FileDoesNotExistException if the key-value store URI does not exists
   */
  public void completeStore(TachyonURI path) throws FileDoesNotExistException {
    final long fileId = mFileSystemMaster.getFileId(path);
    if (fileId == IdUtils.INVALID_FILE_ID) {
      throw new FileDoesNotExistException(
          String.format("Failed to completeStore: path %s does not exist", path));
    }

    List<PartitionInfo> partitions;
    synchronized (mLock) {
      if (!mIncompleteStoreToPartitions.containsKey(fileId)) {
        // TODO(binfan): throw a better exception
        throw new FileDoesNotExistException(
            String.format("Failed to completeStore: KeyValueStore %s does not exist", path));
      }
      partitions = mIncompleteStoreToPartitions.remove(fileId);
      mCompleteStoreToPartitions.put(fileId, partitions);
    }
  }

  /**
   * Creates a new key-value store.
   *
   * @param path URI of the key-value store
   * @throws FileAlreadyExistsException if a key-value store URI exists
   */
  public void createStore(TachyonURI path) throws FileAlreadyExistsException {
    final long fileId = mFileSystemMaster.getFileId(path);
    if (fileId != IdUtils.INVALID_FILE_ID) {
      throw new FileAlreadyExistsException(
          String.format("Failed to createStore: path %s already exists", path));
    }

    synchronized (mLock) {
      if (mIncompleteStoreToPartitions.containsKey(fileId)) {
        // TODO(binfan): throw a better exception
        throw new FileAlreadyExistsException(
            String.format("Failed to createStore: KeyValueStore %s is already created", path));
      }
      mIncompleteStoreToPartitions.put(fileId, Lists.<PartitionInfo>newArrayList());
    }
  }

  /**
   * Gets a list of partitions of a given key-value store.
   *
   * @param path URI of the key-value store
   * @return a list of partition information
   * @throws FileDoesNotExistException if the key-value store URI does not exists
   */
  public List<PartitionInfo> getPartitionInfo(TachyonURI path) throws FileDoesNotExistException {
    final long fileId = mFileSystemMaster.getFileId(path);
    if (fileId == IdUtils.INVALID_FILE_ID) {
      throw new FileDoesNotExistException(
          String.format("Failed to getPartitionInfo: path %s does not exist", path));
    }

    List<PartitionInfo> partitions;
    synchronized (mLock) {
      partitions = mCompleteStoreToPartitions.get(fileId);
    }
    if (partitions == null) {
      return Lists.newArrayList();
    }
    return partitions;
  }
}
