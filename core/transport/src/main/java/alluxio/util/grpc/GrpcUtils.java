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

package alluxio.util.grpc;

import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.TieredIdentity;
import alluxio.wire.TtlAction;
import alluxio.wire.WorkerNetAddress;

import com.google.common.net.HostAndPort;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for conversion between wire types and grpc types.
 */
@ThreadSafe
public final class GrpcUtils {

  private GrpcUtils() {} // prevent instantiation

  /**
   * Converts a proto type to a wire type.
   *
   * @param filePInfo the proto representation of a file information
   * @return wire representation of the file information
   */
  public static FileInfo fromProto(alluxio.grpc.FileInfo filePInfo) {
    FileInfo fileInfo = new FileInfo();
    fileInfo.setFileId(filePInfo.getFileId());
    fileInfo.setName(filePInfo.getName());
    fileInfo.setPath(filePInfo.getPath());
    fileInfo.setUfsPath(filePInfo.getUfsPath());
    fileInfo.setLength(filePInfo.getLength());
    fileInfo.setBlockSizeBytes(filePInfo.getBlockSizeBytes());
    fileInfo.setCreationTimeMs(filePInfo.getCreationTimeMs());
    fileInfo.setCompleted(filePInfo.getCompleted());
    fileInfo.setFolder(filePInfo.getFolder());
    fileInfo.setPinned(filePInfo.getPinned());
    fileInfo.setCacheable(filePInfo.getCacheable());
    fileInfo.setPersisted(filePInfo.getPersisted());
    // TODO(adit): Fill in the rest
//    mBlockIds = new ArrayList<>(filePInfo.getBlockIdsList());
//    mLastModificationTimeMs = filePInfo.getLastModificationTimeMs();
//    mTtl = filePInfo.getTtl();
//    mTtlAction = GrpcUtils.fromProto(filePInfo.getTtlAction());
//    mOwner = filePInfo.getOwner();
//    mGroup = filePInfo.getGroup();
//    mMode = filePInfo.getMode();
//    mPersistenceState = filePInfo.getPersistenceState();
//    mMountPoint = filePInfo.getMountPoint();
//    mFileBlockInfos = new ArrayList<>();
//      for (alluxio.grpc.FileBlockInfo fileBlockInfo : filePInfo.getFileBlockInfosList()) {
//        mFileBlockInfos.add(new FileBlockInfo(fileBlockInfo));
//      }
//    mMountId = filePInfo.getMountId();
//    mInAlluxioPercentage = filePInfo.getInAlluxioPercentage();
//    if (filePInfo.hasUfsFingerprint()) {
//      mUfsFingerprint = filePInfo.getUfsFingerprint();
//    }
    return fileInfo;
  }

  /**
   * Converts thrift type to wire type.
   *
   * @param tTtlAction {@link TTtlAction}
   * @return {@link TtlAction} equivalent
   */
  public static TtlAction fromProto(alluxio.grpc.TtlAction tTtlAction) {
    if (tTtlAction == null) {
      return TtlAction.DELETE;
    }
    switch (tTtlAction) {
      case DELETE:
        return TtlAction.DELETE;
      case FREE:
        return TtlAction.FREE;
      default:
        throw new IllegalStateException("Unrecognized thrift ttl action: " + tTtlAction);
    }
  }

  /**
   * Converts a wire type to a proto type.
   *
   * @param fileInfo the wire representation of a file information
   * @return proto representation of the file information
   */
  public static alluxio.grpc.FileInfo toProto(FileInfo fileInfo) {
        List<alluxio.grpc.FileBlockInfo> fileBlockInfos = new ArrayList<>();
    for (FileBlockInfo fileBlockInfo : fileInfo.getFileBlockInfos()) {
      fileBlockInfos.add(toProto(fileBlockInfo));
    }
    return alluxio.grpc.FileInfo.newBuilder()
            .setFileId(fileInfo.getFileId())
            .setName(fileInfo.getName())
            .setPath(fileInfo.getPath())
            .setUfsPath(fileInfo.getUfsPath())
            // TODO(adit): fill in the rest
//            .setLength(mLength)
//            .setBlockSizeBytes(mBlockSizeBytes)
//            .setCreationTimeMs(mCreationTimeMs)
//            .setCompleted(mCompleted)
//            .setFolder(mFolder)
//            .setPinned(mPinned)
//            .setCacheable(mCacheable)
//            .setPersisted(mPersisted)
//            .addAllBlockIds(mBlockIds)
//            .setLastModificationTimeMs(mLastModificationTimeMs)
//            .setTtl(mTtl)
//            .setOwner(mOwner)
//            .setGroup(mGroup)
//            .setMode(mMode)
//            .setPersistenceState(mPersistenceState)
//            .setMountPoint(mMountPoint)
//            .addAllFileBlockInfos(fileBlockInfos)
//            .setTtlAction(GrpcUtils.toProto(mTtlAction))
//            .setMountId(mMountId)
//            .setInAlluxioPercentage(mInAlluxioPercentage)
//            .setUfsFingerprint(mUfsFingerprint)
            .build();
  }

  /**
   * Converts wire type to thrift type.
   *
   * @param ttlAction {@link TtlAction}
   * @return {@link TTtlAction} equivalent
   */
  public static alluxio.grpc.TtlAction toProto(TtlAction ttlAction) {
    if (ttlAction == null) {
      return alluxio.grpc.TtlAction.DELETE;
    }
    switch (ttlAction) {
      case DELETE:
        return alluxio.grpc.TtlAction.DELETE;
      case FREE:
        return alluxio.grpc.TtlAction.FREE;
      default:
        throw new IllegalStateException("Unrecognized ttl action: " + ttlAction);
    }
  }

  /**
   * Converts wire type to thrift type.
   *
   * @param ttlAction {@link TtlAction}
   * @return {@link TTtlAction} equivalent
   */
  public static alluxio.grpc.FileBlockInfo toProto(FileBlockInfo fileBlockInfo) {
    List<alluxio.grpc.WorkerNetAddress> ufsLocations = new ArrayList<>();
    for (String ufsLocation : fileBlockInfo.getUfsLocations()) {
      HostAndPort address = HostAndPort.fromString(ufsLocation);
      ufsLocations.add(alluxio.grpc.WorkerNetAddress.newBuilder().setHost(address.getHostText())
          .setDataPort(address.getPortOrDefault(-1)).build());
    }
    return alluxio.grpc.FileBlockInfo.newBuilder()
        .setBlockInfo(toProto(fileBlockInfo.getBlockInfo()))
        .setOffset(fileBlockInfo.getOffset())
        .addAllUfsLocations(ufsLocations)
        .addAllUfsStringLocations(fileBlockInfo.getUfsLocations())
        .build();
  }

   /**
   * Converts wire type to thrift type.
   *
   * @param ttlAction {@link TtlAction}
   * @return {@link TTtlAction} equivalent
   */
  public static alluxio.grpc.BlockInfo toProto(BlockInfo blockInfo) {
      List<alluxio.grpc.BlockLocation> locations = new ArrayList<>();
    for (BlockLocation location : blockInfo.getLocations()) {
      locations.add(toProto(location));
    }
    return alluxio.grpc.BlockInfo.newBuilder().setBlockId(blockInfo.getBlockId()).setLength(blockInfo.getLength())
        .addAllLocations(locations).build();
  }

  /**
   * Converts wire type to thrift type.
   *
   * @param ttlAction {@link TtlAction}
   * @return {@link TTtlAction} equivalent
   */
  public static alluxio.grpc.BlockLocation toProto(BlockLocation blockLocation) {
    return alluxio.grpc.BlockLocation.newBuilder()
        .setWorkerId(blockLocation.getWorkerId())
        .setWorkerAddress(toProto(blockLocation.getWorkerAddress()))
        .setTierAlias(blockLocation.getTierAlias())
        .build();
  }
  
  /**
   * Converts wire type to thrift type.
   *
   * @param ttlAction {@link TtlAction}
   * @return {@link TTtlAction} equivalent
   */
  public static alluxio.grpc.WorkerNetAddress toProto(WorkerNetAddress workerNetAddress) {
    alluxio.grpc.WorkerNetAddress.Builder address =
        alluxio.grpc.WorkerNetAddress.newBuilder()
            .setHost(workerNetAddress.getHost())
            .setRpcPort(workerNetAddress.getRpcPort())
            .setDataPort(workerNetAddress.getDataPort())
            .setWebPort(workerNetAddress.getWebPort())
            .setDomainSocketPath(workerNetAddress.getDomainSocketPath());
    if (workerNetAddress.getTieredIdentity() != null) {
      address.setTieredIdentity(toProto(workerNetAddress.getTieredIdentity()));
    }
    return address.build();
  }

  /**
   * Converts wire type to thrift type.
   *
   * @param ttlAction {@link TtlAction}
   * @return {@link TTtlAction} equivalent
   */
  public static alluxio.grpc.TieredIdentity toProto(TieredIdentity tieredIdentity) {
    return alluxio.grpc.TieredIdentity.newBuilder()
        .addAllTiers(tieredIdentity.getTiers().stream().map(GrpcUtils::toProto)
            .collect(Collectors.toList()))
        .build();
  }

  /**
   * Converts wire type to thrift type.
   *
   * @param ttlAction {@link TtlAction}
   * @return {@link TTtlAction} equivalent
   */
  public static alluxio.grpc.LocalityTier toProto(TieredIdentity.LocalityTier localityTier) {
      return alluxio.grpc.LocalityTier.newBuilder()
          .setTierName(localityTier.getTierName())
          .setValue(localityTier.getValue())
          .build();
  }
}

