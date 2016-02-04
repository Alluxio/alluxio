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

package tachyon.wire;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Lists;

/**
 * Utility methods for conversion between wire types and thrift types.
 */
@ThreadSafe
public final class ThriftUtils {

  /**
   * Converts a thrift type to a wire type.
   *
   * @param blockInfo the thrift representation of a block descriptor
   * @return wire representation of the block descriptor
   */
  public static BlockInfo fromThrift(tachyon.thrift.BlockInfo blockInfo) {
    BlockInfo result = new BlockInfo();
    result.setBlockId(blockInfo.getBlockId());
    result.setLength(blockInfo.getLength());
    List<BlockLocation> locations = Lists.newArrayList();
    for (tachyon.thrift.BlockLocation location : blockInfo.getLocations()) {
      locations.add(fromThrift(location));
    }
    result.setLocations(locations);
    return result;
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param blockLocation the thrift representation of a block location
   * @return wire representation of the block location
   */
  public static BlockLocation fromThrift(tachyon.thrift.BlockLocation blockLocation) {
    BlockLocation result = new BlockLocation();
    result.setWorkerId(blockLocation.getWorkerId());
    result.setWorkerAddress(fromThrift(blockLocation.getWorkerAddress()));
    result.setTierAlias(blockLocation.getTierAlias());
    return result;
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param jobInfo the thrift representation of a command-line job descriptor
   * @return wire representation of the command-line job descriptor
   */
  public static CommandLineJobInfo fromThrift(tachyon.thrift.CommandLineJobInfo jobInfo) {
    CommandLineJobInfo result = new CommandLineJobInfo();
    result.setCommand(jobInfo.getCommand());
    result.setConf(fromThrift(jobInfo.getConf()));
    return result;
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param fileBlockInfo the thrift representation of a file block descriptor
   * @return wire representation of the file block descriptor
   */
  public static FileBlockInfo fromThrift(tachyon.thrift.FileBlockInfo fileBlockInfo) {
    FileBlockInfo result = new FileBlockInfo();
    result.setBlockInfo(fromThrift(fileBlockInfo.getBlockInfo()));
    result.setOffset(fileBlockInfo.getOffset());
    List<WorkerNetAddress> locations = Lists.newArrayList();
    for (tachyon.thrift.WorkerNetAddress location : fileBlockInfo.getUfsLocations()) {
      locations.add(fromThrift(location));
    }
    result.setUfsLocations(locations);
    return result;
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param fileInfo the thrift representation of a file descriptor
   * @return wire representation of the file descriptor
   */
  public static FileInfo fromThrift(tachyon.thrift.FileInfo fileInfo) {
    FileInfo result = new FileInfo();
    result.setFileId(fileInfo.getFileId());
    result.setName(fileInfo.getName());
    result.setPath(fileInfo.getPath());
    result.setUfsPath(fileInfo.getUfsPath());
    result.setLength(fileInfo.getLength());
    result.setBlockSizeBytes(fileInfo.getBlockSizeBytes());
    result.setCreationTimeMs(fileInfo.getCreationTimeMs());
    result.setCompleted(fileInfo.isCompleted());
    result.setFolder(fileInfo.isFolder());
    result.setPinned(fileInfo.isPinned());
    result.setCacheable(fileInfo.isCacheable());
    result.setPersisted(fileInfo.isPersisted());
    result.setBlockIds(fileInfo.getBlockIds());
    result.setInMemoryPercentage(fileInfo.getInMemoryPercentage());
    result.setLastModificationTimeMs(fileInfo.getLastModificationTimeMs());
    result.setTtl(fileInfo.getTtl());
    result.setUserName(fileInfo.getUserName());
    result.setGroupName(fileInfo.getGroupName());
    result.setPermission(fileInfo.getPermission());
    result.setPersistenceState(fileInfo.getPersistenceState());
    return result;
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param conf the thrift representation of a command-line job configuration
   * @return wire representation of the command-line job configuration
   */
  public static JobConfInfo fromThrift(tachyon.thrift.JobConfInfo conf) {
    JobConfInfo result = new JobConfInfo();
    result.setOutputFile(conf.getOutputFile());
    return result;
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param lineageInfo the thrift representation of a lineage descriptor
   * @return wire representation of the lineage descriptor
   */
  public static LineageInfo fromThrift(tachyon.thrift.LineageInfo lineageInfo) {
    LineageInfo result = new LineageInfo();
    result.setId(lineageInfo.getId());
    result.setInputFiles(lineageInfo.getInputFiles());
    result.setOutputFiles(lineageInfo.getOutputFiles());
    result.setJob(fromThrift(lineageInfo.getJob()));
    result.setCreationTimeMs(lineageInfo.getCreationTimeMs());
    result.setParents(lineageInfo.getParents());
    result.setChildren(lineageInfo.getChildren());
    return result;
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param lockBlockResult the thrift representation of a lock block operation result
   * @return wire representation of the lock block operation result
   */
  public static LockBlockResult fromThrift(tachyon.thrift.LockBlockResult lockBlockResult) {
    LockBlockResult result = new LockBlockResult();
    result.setLockId(lockBlockResult.getLockId());
    result.setBlockPath(lockBlockResult.getBlockPath());
    return result;
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param workerInfo the thrift representation of a worker descriptor
   * @return wire representation of the worker descriptor
   */
  public static WorkerInfo fromThrift(tachyon.thrift.WorkerInfo workerInfo) {
    WorkerInfo result = new WorkerInfo();
    result.setId(workerInfo.getId());
    result.setAddress(fromThrift(workerInfo.getAddress()));
    result.setLastContactSec(workerInfo.getLastContactSec());
    result.setState((workerInfo.getState()));
    result.setCapacityBytes(workerInfo.getCapacityBytes());
    result.setUsedBytes(workerInfo.getUsedBytes());
    result.setStartTimeMs(workerInfo.getStartTimeMs());
    return result;
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param workerNetAddress the thrift representation of a worker net address
   * @return wire representation of the worker net address
   */
  public static WorkerNetAddress fromThrift(tachyon.thrift.WorkerNetAddress workerNetAddress) {
    WorkerNetAddress result = new WorkerNetAddress();
    result.setHost(workerNetAddress.getHost());
    result.setRpcPort(workerNetAddress.getRpcPort());
    result.setDataPort(workerNetAddress.getDataPort());
    result.setWebPort(workerNetAddress.getWebPort());
    return result;
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param blockInfo the wire representation of a block descriptor
   * @return thrift representation of the block descriptor
   */
  public static tachyon.thrift.BlockInfo toThrift(BlockInfo blockInfo) {
    List<tachyon.thrift.BlockLocation> locations = Lists.newArrayList();
    for (BlockLocation location : blockInfo.getLocations()) {
      locations.add(toThrift(location));
    }
    return new tachyon.thrift.BlockInfo(blockInfo.getBlockId(), blockInfo.getLength(), locations);
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param blockLocation the wire representation of a block location
   * @return thrift representation of the block location
   */
  public static tachyon.thrift.BlockLocation toThrift(BlockLocation blockLocation) {
    return new tachyon.thrift.BlockLocation(blockLocation.getWorkerId(),
        toThrift(blockLocation.getWorkerAddress()), blockLocation.getTierAlias());
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param jobInfo the wire representation of a command-line job descriptor
   * @return thrift representation of the command-line job descriptor
   */
  public static tachyon.thrift.CommandLineJobInfo toThrift(CommandLineJobInfo jobInfo) {
    return new tachyon.thrift.CommandLineJobInfo(jobInfo.getCommand(), toThrift(jobInfo.getConf()));
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param fileBlockInfo the wire representation of a file block descriptor
   * @return thrift representation of the command-line job descriptor
   */
  public static tachyon.thrift.FileBlockInfo toThrift(FileBlockInfo fileBlockInfo) {
    List<tachyon.thrift.WorkerNetAddress> locations = Lists.newArrayList();
    for (WorkerNetAddress location : fileBlockInfo.getUfsLocations()) {
      locations.add(toThrift(location));
    }
    return new tachyon.thrift.FileBlockInfo(toThrift(fileBlockInfo.getBlockInfo()),
        fileBlockInfo.getOffset(), locations);
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param fileInfo the wire representation of a file descriptor
   * @return thrift representation of the file descriptor
   */
  public static tachyon.thrift.FileInfo toThrift(FileInfo fileInfo) {
    return new tachyon.thrift.FileInfo(fileInfo.getFileId(), fileInfo.getName(),
        fileInfo.getPath(), fileInfo.getUfsPath(), fileInfo.getLength(),
        fileInfo.getBlockSizeBytes(), fileInfo.getCreationTimeMs(), fileInfo.isCompleted(),
        fileInfo.isFolder(), fileInfo.isPinned(), fileInfo.isCacheable(), fileInfo.isPersisted(),
        fileInfo.getBlockIds(), fileInfo.getInMemoryPercentage(),
        fileInfo.getLastModificationTimeMs(), fileInfo.getTtl(), fileInfo.getUserName(),
        fileInfo.getGroupName(), fileInfo.getPermission(), fileInfo.getPersistenceState());
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param conf the wire representation of a command-line job configuration
   * @return thrift representation of the command-line job configuration
   */
  public static tachyon.thrift.JobConfInfo toThrift(JobConfInfo conf) {
    return new tachyon.thrift.JobConfInfo(conf.getOutputFile());
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param lineageInfo the wire representation of a lineage descriptor
   * @return thrift representation of the lineage descriptor
   */
  public static tachyon.thrift.LineageInfo toThrift(LineageInfo lineageInfo) {
    return new tachyon.thrift.LineageInfo(lineageInfo.getId(), lineageInfo.getInputFiles(),
        lineageInfo.getOutputFiles(), toThrift(lineageInfo.getJob()),
        lineageInfo.getCreationTimeMs(), lineageInfo.getParents(), lineageInfo.getChildren());
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param lockBlockResult the wire representation of a lock block operation result
   * @return thrift representation of the lock block operation result
   */
  public static tachyon.thrift.LockBlockResult toThrift(LockBlockResult lockBlockResult) {
    return new tachyon.thrift.LockBlockResult(lockBlockResult.getLockId(),
        lockBlockResult.getBlockPath());
  }

  /**
   * Converts a wire type to a thrift type
   *
   * @param workerInfo the wire representation of a worker descriptor
   * @return thrift representation of the worker descriptor
   */
  public static tachyon.thrift.WorkerInfo toThrift(WorkerInfo workerInfo) {
    return new tachyon.thrift.WorkerInfo(workerInfo.getId(), toThrift(workerInfo.getAddress()),
        workerInfo.getLastContactSec(), workerInfo.getState(), workerInfo.getCapacityBytes(),
        workerInfo.getUsedBytes(), workerInfo.getStartTimeMs());
  }

  /**
   * Converts a wire type to a thrift type
   *
   * @param workerNetAddress the wire representation of a worker net address
   * @return thrift representation of the worker net address
   */
  public static tachyon.thrift.WorkerNetAddress toThrift(WorkerNetAddress workerNetAddress) {
    return new tachyon.thrift.WorkerNetAddress(workerNetAddress.getHost(),
        workerNetAddress.getRpcPort(), workerNetAddress.getDataPort(),
        workerNetAddress.getWebPort());
  }
}

