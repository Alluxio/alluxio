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
import tachyon.thrift.PartitionInfo;
import tachyon.util.IdUtils;
import tachyon.util.io.PathUtils;

/**
 * The key-value master stores key-value store information in Tachyon, including the partitions of
 * each key-value store.
 */
public class KeyValueMaster extends MasterBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final FileSystemMaster mFileSystemMaster;

  /** Map from fileId of a store (dir) to the list of partitions in this store. */
  private final Map<Long, List<PartitionInfo>> mCompleteStoreToPartitions;
  private final Map<Long, List<PartitionInfo>> mIncompleteStoreToPartitions;

  /**
   * @param baseDirectory the base journal directory
   * @return the journal directory for this master
   */
  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, Constants.KEY_VALUE_MASTER_NAME);
  }

  public KeyValueMaster(FileSystemMaster fileSystemMaster, Journal journal) {
    super(journal, 2);
    mFileSystemMaster = fileSystemMaster;
    mCompleteStoreToPartitions = Maps.newHashMap();
    mIncompleteStoreToPartitions = Maps.newHashMap();
  }

  @Override
  public Map<String, TProcessor> getServices() {
    return null;
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

  public void completePartition(TachyonURI path, PartitionInfo info)
      throws FileDoesNotExistException {
    final long fileId = mFileSystemMaster.getFileId(path);
    if (fileId == IdUtils.INVALID_FILE_ID) {
      LOG.error("Failed to completeStore: path {} doesnot exist", path);
      throw new FileDoesNotExistException("fill me");
    }

    synchronized (mIncompleteStoreToPartitions) {
      if (!mIncompleteStoreToPartitions.containsKey(fileId)) {
        // TODO(binfan): throw a better exception
        throw new FileDoesNotExistException("fill me");
      }
      mIncompleteStoreToPartitions.get(fileId).add(info);
    }
  }

  public void completeStore(TachyonURI path) throws FileDoesNotExistException {
    final long fileId = mFileSystemMaster.getFileId(path);
    if (fileId == IdUtils.INVALID_FILE_ID) {
      LOG.error("Failed to completeStore: path {} doesnot exist", path);
      throw new FileDoesNotExistException("fill me");
    }
    List<PartitionInfo> partitions;
    synchronized (mIncompleteStoreToPartitions) {
      partitions = mIncompleteStoreToPartitions.remove(fileId);
    }
    synchronized (mCompleteStoreToPartitions) {
      mCompleteStoreToPartitions.put(fileId, partitions);
    }
  }

  public void createStore(TachyonURI path) throws FileAlreadyExistsException {
    final long fileId = mFileSystemMaster.getFileId(path);
    if (fileId != IdUtils.INVALID_FILE_ID) {
      LOG.error("Failed to createStore: path {} already exists", path);
      throw new FileAlreadyExistsException("fill me");
    }

    synchronized (mIncompleteStoreToPartitions) {
      if (mIncompleteStoreToPartitions.containsKey(fileId)) {
        // TODO(binfan): throw a better exception
        LOG.error("Failed to createStore: KeyValueStore {} already created", path);
        throw new FileAlreadyExistsException("fill me");
      }
      mIncompleteStoreToPartitions.put(fileId, Lists.<PartitionInfo>newArrayList());
    }
  }

  public List<PartitionInfo> getPartitionInfo(TachyonURI path) throws FileDoesNotExistException {
    final long fileId = mFileSystemMaster.getFileId(path);
    if (fileId == IdUtils.INVALID_FILE_ID) {
      LOG.error("Failed to getPartitionInfo: path {} doesnot exist", path);
      throw new FileDoesNotExistException("fill me");
    }
    List<PartitionInfo> partitions;
    synchronized (mCompleteStoreToPartitions) {
      partitions = mCompleteStoreToPartitions.get(fileId);
    }
    if (partitions == null) {
      return Lists.newArrayList();
    }
    return partitions;
  }
}
