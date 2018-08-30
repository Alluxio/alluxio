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

import alluxio.file.options.CheckConsistencyOptions;
import alluxio.file.options.CommonOptions;
import alluxio.file.options.CompleteFileOptions;
import alluxio.file.options.CreateDirectoryOptions;
import alluxio.file.options.CreateFileOptions;
import alluxio.file.options.DeleteOptions;
import alluxio.file.options.FreeOptions;
import alluxio.file.options.GetStatusOptions;
import alluxio.file.options.ListStatusOptions;
import alluxio.file.options.MountOptions;
import alluxio.file.options.RenameOptions;
import alluxio.file.options.SetAttributeOptions;
import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.master.file.FileSystemMasterOptions;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.MountPointInfo;
import alluxio.wire.TieredIdentity;
import alluxio.wire.TtlAction;
import alluxio.wire.WorkerNetAddress;

import com.google.common.net.HostAndPort;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for conversion between wire types and grpc types.
 */
@ThreadSafe
public final class GrpcUtils {

  private GrpcUtils() {} // prevent instantiation

  /**
   * Converts from proto type to options.
   */
  public static CheckConsistencyOptions fromProto(FileSystemMasterOptions masterOptions, CheckConsistencyPOptions pOptions) {
    return null;
  }

  /**
   * Converts from proto type to options.
   */
  public static CommonOptions fromProto(FileSystemMasterOptions masterOptions,
      FileSystemMasterCommonPOptions pOptions) {
    CommonOptions options = masterOptions.getCommonOptions();
    if (pOptions != null) {
      if (pOptions.hasSyncIntervalMs()) {
        options.setSyncIntervalMs(pOptions.getSyncIntervalMs());
      }
    }
    return options;
  }

  /**
   * Converts from proto type to options.
   */
  public static CompleteFileOptions fromProto(FileSystemMasterOptions masterOptions, CompleteFilePOptions pOptions) {
    return null;
  }

  /**
   * Converts from proto type to options.
   */
  public static CreateDirectoryOptions fromProto(FileSystemMasterOptions masterOptions, CreateDirectoryPOptions pOptions) {
    return null;
  }

  /**
   * Converts from proto type to options.
   */
  public static CreateFileOptions fromProto(FileSystemMasterOptions masterOptions, CreateFilePOptions pOptions) {
    return null;
  }

  /**
   * Converts from proto type to options.
   */
  public static FreeOptions fromProto(FileSystemMasterOptions masterOptions, FreePOptions pOptions) {
    return null;
  }

  /**
   * Converts from proto type to options.
   */
  public static GetStatusOptions fromProto(FileSystemMasterOptions masterOptions, GetStatusPOptions pOptions) {
    GetStatusOptions options = masterOptions.getGetStatusOptions();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        options.setCommonOptions(fromProto(masterOptions, pOptions.getCommonOptions()));
      }
      if (pOptions.hasLoadMetadataType()) {
        options.setLoadMetadataType(fromProto(pOptions.getLoadMetadataType()));
      }
    }
    return options;
  }

  /**
   * Converts from proto type to options.
   */
  public static ListStatusOptions fromProto(FileSystemMasterOptions masterOptions, ListStatusPOptions pOptions) {
    return null;
  }

  /**
   * Converts from proto type to options.
   *
   * @param loadMetadataTType the proto representation of loadMetadataType
   * @return the {@link LoadMetadataType}
   */
  @Nullable
  public static LoadMetadataType fromProto(LoadMetadataPType loadMetadataPType) {
    switch (loadMetadataPType) {
      case NEVER:
        return LoadMetadataType.Never;
      case ONCE:
        return LoadMetadataType.Once;
      case ALWAYS:
        return LoadMetadataType.Always;
      default:
        return null;
    }
  }

  /**
   * Converts from proto type to options.
   */
  public static MountOptions fromProto(FileSystemMasterOptions masterOptions, MountPOptions pOptions) {
    return null;
  }

  /**
   * Converts from proto type to options.
   */
  public static DeleteOptions fromProto(FileSystemMasterOptions masterOptions, DeletePOptions pOptions) {
    return null;
  }

  /**
   * Converts from proto type to options.
   */
  public static RenameOptions fromProto(FileSystemMasterOptions masterOptions, RenamePOptions pOptions) {
    return null;
  }

  /**
   * Converts from proto type to options.
   */
  public static SetAttributeOptions fromProto(FileSystemMasterOptions masterOptions, SetAttributePOptions pOptions) {
    return null;
  }

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
   * Converts proto type to wire type.
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
        throw new IllegalStateException("Unrecognized proto ttl action: " + tTtlAction);
    }
  }

  /**
   * Converts a wire type to a proto type.
   */
  public static alluxio.grpc.MountPointInfo toProto(MountPointInfo fileInfo) {
    return null;
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
   * Converts wire type to proto type.
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
   * Converts wire type to proto type.
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
   * Converts wire type to proto type.
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
   * Converts wire type to proto type.
   */
  public static alluxio.grpc.BlockLocation toProto(BlockLocation blockLocation) {
    return alluxio.grpc.BlockLocation.newBuilder()
        .setWorkerId(blockLocation.getWorkerId())
        .setWorkerAddress(toProto(blockLocation.getWorkerAddress()))
        .setTierAlias(blockLocation.getTierAlias())
        .build();
  }
  
  /**
   * Converts wire type to proto type.
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
   * Converts wire type to proto type.
   */
  public static alluxio.grpc.TieredIdentity toProto(TieredIdentity tieredIdentity) {
    return alluxio.grpc.TieredIdentity.newBuilder()
        .addAllTiers(tieredIdentity.getTiers().stream().map(GrpcUtils::toProto)
            .collect(Collectors.toList()))
        .build();
  }

  /**
   * Converts wire type to proto type.
   */
  public static alluxio.grpc.LocalityTier toProto(TieredIdentity.LocalityTier localityTier) {
      return alluxio.grpc.LocalityTier.newBuilder()
          .setTierName(localityTier.getTierName())
          .setValue(localityTier.getValue())
          .build();
  }

  /**
   * Converts options to proto type.
   */
  public static GetStatusPOptions toProto(GetStatusOptions options) {
    return GetStatusPOptions.newBuilder()
        .setLoadMetadataType(toProto(options.getLoadMetadataType()))
        .setCommonOptions(toProto(options.getCommonOptions()))
        .build();
  }

  /**
   * Converts options to proto type.
   */
  public static FileSystemMasterCommonPOptions toProto(CommonOptions options) {
    return FileSystemMasterCommonPOptions.newBuilder()
        .setSyncIntervalMs(options.getSyncIntervalMs())
        .build();
  }

  /**
   * Converts options to proto type.
   *
   * @param loadMetadataType the {@link LoadMetadataType}
   * @return the proto representation of this enum
   */
  public static LoadMetadataPType toProto(LoadMetadataType loadMetadataType) {
    return LoadMetadataPType.forNumber(loadMetadataType.getValue());
  }

}

