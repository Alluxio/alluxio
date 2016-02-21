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

package alluxio.wire;

import javax.annotation.concurrent.ThreadSafe;

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
  public static BlockInfo fromThrift(alluxio.thrift.BlockInfo blockInfo) {
    return new BlockInfo(blockInfo);
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param blockLocation the thrift representation of a block location
   * @return wire representation of the block location
   */
  public static BlockLocation fromThrift(alluxio.thrift.BlockLocation blockLocation) {
    return new BlockLocation(blockLocation);
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param jobInfo the thrift representation of a command-line job descriptor
   * @return wire representation of the command-line job descriptor
   */
  public static CommandLineJobInfo fromThrift(alluxio.thrift.CommandLineJobInfo jobInfo) {
    return new CommandLineJobInfo(jobInfo);
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param fileBlockInfo the thrift representation of a file block descriptor
   * @return wire representation of the file block descriptor
   */
  public static FileBlockInfo fromThrift(alluxio.thrift.FileBlockInfo fileBlockInfo) {
    return new FileBlockInfo(fileBlockInfo);
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param fileInfo the thrift representation of a file descriptor
   * @return wire representation of the file descriptor
   */
  public static FileInfo fromThrift(alluxio.thrift.FileInfo fileInfo) {
    return new FileInfo(fileInfo);
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param conf the thrift representation of a command-line job configuration
   * @return wire representation of the command-line job configuration
   */
  public static JobConfInfo fromThrift(alluxio.thrift.JobConfInfo conf) {
    return new JobConfInfo(conf);
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param lineageInfo the thrift representation of a lineage descriptor
   * @return wire representation of the lineage descriptor
   */
  public static LineageInfo fromThrift(alluxio.thrift.LineageInfo lineageInfo) {
    return new LineageInfo(lineageInfo);
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param lockBlockResult the thrift representation of a lock block operation result
   * @return wire representation of the lock block operation result
   */
  public static LockBlockResult fromThrift(alluxio.thrift.LockBlockResult lockBlockResult) {
    return new LockBlockResult(lockBlockResult);
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param workerInfo the thrift representation of a worker descriptor
   * @return wire representation of the worker descriptor
   */
  public static WorkerInfo fromThrift(alluxio.thrift.WorkerInfo workerInfo) {
    return new WorkerInfo(workerInfo);
  }

  /**
   * Converts a thrift type to a wire type.
   *
   * @param workerNetAddress the thrift representation of a worker net address
   * @return wire representation of the worker net address
   */
  public static WorkerNetAddress fromThrift(alluxio.thrift.WorkerNetAddress workerNetAddress) {
    return new WorkerNetAddress(workerNetAddress);
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param blockInfo the wire representation of a block descriptor
   * @return thrift representation of the block descriptor
   */
  public static alluxio.thrift.BlockInfo toThrift(BlockInfo blockInfo) {
    return blockInfo.toThrift();
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param blockLocation the wire representation of a block location
   * @return thrift representation of the block location
   */
  public static alluxio.thrift.BlockLocation toThrift(BlockLocation blockLocation) {
    return blockLocation.toThrift();
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param jobInfo the wire representation of a command-line job descriptor
   * @return thrift representation of the command-line job descriptor
   */
  public static alluxio.thrift.CommandLineJobInfo toThrift(CommandLineJobInfo jobInfo) {
    return jobInfo.toThrift();
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param fileBlockInfo the wire representation of a file block descriptor
   * @return thrift representation of the command-line job descriptor
   */
  public static alluxio.thrift.FileBlockInfo toThrift(FileBlockInfo fileBlockInfo) {
    return fileBlockInfo.toThrift();
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param fileInfo the wire representation of a file descriptor
   * @return thrift representation of the file descriptor
   */
  public static alluxio.thrift.FileInfo toThrift(FileInfo fileInfo) {
    return fileInfo.toThrift();
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param conf the wire representation of a command-line job configuration
   * @return thrift representation of the command-line job configuration
   */
  public static alluxio.thrift.JobConfInfo toThrift(JobConfInfo conf) {
    return conf.toThrift();
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param lineageInfo the wire representation of a lineage descriptor
   * @return thrift representation of the lineage descriptor
   */
  public static alluxio.thrift.LineageInfo toThrift(LineageInfo lineageInfo) {
    return lineageInfo.toThrift();
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param lockBlockResult the wire representation of a lock block operation result
   * @return thrift representation of the lock block operation result
   */
  public static alluxio.thrift.LockBlockResult toThrift(LockBlockResult lockBlockResult) {
    return lockBlockResult.toThrift();
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param workerInfo the wire representation of a worker descriptor
   * @return thrift representation of the worker descriptor
   */
  public static alluxio.thrift.WorkerInfo toThrift(WorkerInfo workerInfo) {
    return workerInfo.toThrift();
  }

  /**
   * Converts a wire type to a thrift type.
   *
   * @param workerNetAddress the wire representation of a worker net address
   * @return thrift representation of the worker net address
   */
  public static alluxio.thrift.WorkerNetAddress toThrift(WorkerNetAddress workerNetAddress) {
    return workerNetAddress.toThrift();
  }
}

