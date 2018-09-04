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
import alluxio.file.options.UpdateUfsModeOptions;
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
import alluxio.grpc.UfsMode;
import alluxio.grpc.UpdateUfsModePOptions;
import alluxio.master.file.FileSystemMasterOptions;
import alluxio.security.authorization.Mode;
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
  public static CheckConsistencyOptions fromProto(FileSystemMasterOptions masterOptions,
      CheckConsistencyPOptions pOptions) {
    CheckConsistencyOptions options = masterOptions.getCheckConsistencyOptions();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        options.setCommonOptions(fromProto(masterOptions, pOptions.getCommonOptions()));
      }
    }
    return options;
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
  public static CompleteFileOptions fromProto(FileSystemMasterOptions masterOptions,
      CompleteFilePOptions pOptions) {
    CompleteFileOptions options = masterOptions.getCompleteFileOptions();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        options.setCommonOptions(fromProto(masterOptions, pOptions.getCommonOptions()));
      }
      options.setUfsLength(pOptions.getUfsLength());
    }
    return options;
  }

  /**
   * Converts from proto type to options.
   */
  public static CreateDirectoryOptions fromProto(FileSystemMasterOptions masterOptions,
      CreateDirectoryPOptions pOptions) {
    CreateDirectoryOptions options = masterOptions.getCreateDirectoryOptions();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        options.setCommonOptions(fromProto(masterOptions, pOptions.getCommonOptions()));
      }
      options.setAllowExists(pOptions.getAllowExist());
      options.setPersisted(pOptions.getPersisted());
      options.setRecursive(pOptions.getRecursive());
      options.setTtl(pOptions.getTtl());
      options.setTtlAction(fromProto(pOptions.getTtlAction()));
      // TODO(adit): implement auth
      // if (SecurityUtils.isAuthenticationEnabled()) {
      // mOwner = SecurityUtils.getOwnerFromThriftClient();
      // mGroup = SecurityUtils.getGroupFromThriftClient();
      // }
      if (pOptions.hasMode()) {
        options.setMode(new Mode((short) pOptions.getMode()));
        // } else {
        // mMode.applyDirectoryUMask();
      }
    }
    return options;
  }

  /**
   * Converts from proto type to options.
   */
  public static CreateFileOptions fromProto(FileSystemMasterOptions masterOptions,
      CreateFilePOptions pOptions) {
    CreateFileOptions options = masterOptions.getCreateFileOptions();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        options.setCommonOptions(fromProto(masterOptions, pOptions.getCommonOptions()));
      }
      options.setBlockSizeBytes(pOptions.getBlockSizeBytes());
      options.setPersisted(pOptions.getPersisted());
      options.setRecursive(pOptions.getRecursive());
      options.setTtl(pOptions.getTtl());
      options.setTtlAction(fromProto(pOptions.getTtlAction()));
      // TODO(adit): implement auth
      // if (SecurityUtils.isAuthenticationEnabled()) {
      // mOwner = SecurityUtils.getOwnerFromThriftClient();
      // mGroup = SecurityUtils.getGroupFromThriftClient();
      // }
      if (pOptions.hasMode()) {
        options.setMode(new Mode((short) pOptions.getMode()));
        // } else {
        // mMode.applyFileUMask();
      }
    }
    return options;

  }

  /**
   * Converts from proto type to options.
   */
  public static FreeOptions fromProto(FileSystemMasterOptions masterOptions,
      FreePOptions pOptions) {
    FreeOptions options = masterOptions.getFreeOptions();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        options.setCommonOptions(fromProto(masterOptions, pOptions.getCommonOptions()));
      }
      options.setForced(pOptions.getForced());
      options.setRecursive(pOptions.getRecursive());
    }
    return options;

  }

  /**
   * Converts from proto type to options.
   */
  public static GetStatusOptions fromProto(FileSystemMasterOptions masterOptions,
      GetStatusPOptions pOptions) {
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
  public static ListStatusOptions fromProto(FileSystemMasterOptions masterOptions,
      ListStatusPOptions pOptions) {
    ListStatusOptions options = masterOptions.getListStatusOptions();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        options.setCommonOptions(fromProto(masterOptions, pOptions.getCommonOptions()));
      }
      if (pOptions.hasLoadMetadataType()) {
        options.setLoadMetadataType(fromProto(pOptions.getLoadMetadataType()));
      } else if (pOptions.hasLoadDirectChildren()) {
        options.setLoadMetadataType(LoadMetadataType.Never);
      }
    }
    return options;

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
  public static MountOptions fromProto(FileSystemMasterOptions masterOptions,
      MountPOptions pOptions) {
    MountOptions options = masterOptions.getMountOptions();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        options.setCommonOptions(fromProto(masterOptions, pOptions.getCommonOptions()));
      }
      if (pOptions.hasReadOnly()) {
        options.setReadOnly(pOptions.getReadOnly());
      }
      if (pOptions.getPropertiesMap() != null) {
        options.getProperties().putAll(pOptions.getPropertiesMap());
      }
      if (pOptions.getShared()) {
        options.setShared(pOptions.getShared());
      }
    }
    return options;
  }

  /**
   * Converts from proto type to options.
   */
  public static DeleteOptions fromProto(FileSystemMasterOptions masterOptions,
      DeletePOptions pOptions) {
    DeleteOptions options = masterOptions.getDeleteOptions();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        options.setCommonOptions(fromProto(masterOptions, pOptions.getCommonOptions()));
      }
      options.setRecursive(pOptions.getRecursive());
      options.setAlluxioOnly(pOptions.getAlluxioOnly());
      options.setUnchecked(pOptions.getUnchecked());
    }
    return options;
  }

  /**
   * Converts from proto type to options.
   */
  public static RenameOptions fromProto(FileSystemMasterOptions masterOptions,
      RenamePOptions pOptions) {
    RenameOptions options = masterOptions.getRenameOptions();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        options.setCommonOptions(fromProto(masterOptions, pOptions.getCommonOptions()));
      }
    }
    return options;
  }

  /**
   * Converts from proto type to options.
   */
  public static SetAttributeOptions fromProto(FileSystemMasterOptions masterOptions,
      SetAttributePOptions pOptions) {
    SetAttributeOptions options = masterOptions.getSetAttributeOptions();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        options.setCommonOptions(fromProto(masterOptions, pOptions.getCommonOptions()));
      }
      if (pOptions.hasPinned()) {
        options.setPinned(pOptions.getPinned());
      }
      if (pOptions.hasTtl()) {
        options.setTtl(pOptions.getTtl());
      }
      if (pOptions.hasTtlAction()) {
        options.setTtlAction(fromProto(pOptions.getTtlAction()));
      }
      if (pOptions.hasPersisted()) {
        options.setPersisted(pOptions.getPersisted());
      }
      if (pOptions.hasOwner()) {
        options.setOwner(pOptions.getOwner());
      }
      if (pOptions.hasGroup()) {
        options.setGroup(pOptions.getGroup());
      }
      if (pOptions.hasMode()) {
        options.setMode((short) pOptions.getMode());
      }
      options.setRecursive(pOptions.getRecursive());
    }
    return options;
  }

  /**
   * Converts a proto type to a wire type.
   */
  public static BlockLocation fromProto(alluxio.grpc.BlockLocation blockPLocation) {
    BlockLocation blockLocation = new BlockLocation();
    blockLocation.setWorkerId(blockPLocation.getWorkerId());
    blockLocation.setWorkerAddress(fromProto(blockPLocation.getWorkerAddress()));
    blockLocation.setTierAlias(blockPLocation.getTierAlias());
    return blockLocation;
  }

  /**
   * Converts a proto type to a wire type.
   */
  public static BlockInfo fromProto(alluxio.grpc.BlockInfo blockPInfo) {
    BlockInfo blockInfo = new BlockInfo();
    blockInfo.setBlockId(blockPInfo.getBlockId());
    blockInfo.setLength(blockPInfo.getLength());
    List<BlockLocation> blockLocations = new ArrayList<>();
    for (alluxio.grpc.BlockLocation location : blockPInfo.getLocationsList()) {
      blockLocations.add(fromProto(location));
    }
    blockInfo.setLocations(blockLocations);
    return blockInfo;
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
    fileInfo.setBlockIds(filePInfo.getBlockIdsList());
    fileInfo.setLastModificationTimeMs(filePInfo.getLastModificationTimeMs());
    fileInfo.setTtl(filePInfo.getTtl());
    fileInfo.setTtlAction(fromProto(filePInfo.getTtlAction()));
    fileInfo.setOwner(filePInfo.getOwner());
    fileInfo.setGroup(filePInfo.getGroup());
    fileInfo.setMode(filePInfo.getMode());
    fileInfo.setPersistenceState(filePInfo.getPersistenceState());
    fileInfo.setMountPoint(filePInfo.getMountPoint());
    List<FileBlockInfo> fileBlockInfos = new ArrayList<>();
    for (alluxio.grpc.FileBlockInfo fileBlockInfo : filePInfo.getFileBlockInfosList()) {
      fileBlockInfos.add(fromProto(fileBlockInfo));
    }
    fileInfo.setFileBlockInfos(fileBlockInfos);
    fileInfo.setMountId(filePInfo.getMountId());
    fileInfo.setInAlluxioPercentage(filePInfo.getInAlluxioPercentage());
    if (filePInfo.hasUfsFingerprint()) {
      fileInfo.setUfsFingerprint(filePInfo.getUfsFingerprint());
    }
    return fileInfo;
  }

  /**
   * Converts a proto type to a wire type.
   */
  public static FileBlockInfo fromProto(alluxio.grpc.FileBlockInfo fileBlockPInfo) {
    FileBlockInfo fileBlockInfo = new FileBlockInfo();
    fileBlockInfo.setBlockInfo(fromProto(fileBlockPInfo.getBlockInfo()));
    fileBlockInfo.setOffset(fileBlockInfo.getOffset());
    if (fileBlockPInfo.getUfsStringLocationsCount() != 0) {
      fileBlockInfo.setUfsLocations(new ArrayList<>(fileBlockPInfo.getUfsStringLocationsList()));
    } else if (fileBlockPInfo.getUfsLocationsCount() != 0) {
      for (alluxio.grpc.WorkerNetAddress address : fileBlockPInfo.getUfsLocationsList()) {
        fileBlockInfo.getUfsLocations()
            .add(HostAndPort.fromParts(address.getHost(), address.getDataPort()).toString());
      }
    }
    return fileBlockInfo;
  }

  /**
   * Converts a proto type to a wire type.
   */
  public static TieredIdentity fromProto(alluxio.grpc.TieredIdentity tieredPIdentity) {
    return new TieredIdentity(tieredPIdentity.getTiersList().stream().map(GrpcUtils::fromProto)
        .collect(Collectors.toList()));
  }

  /**
   * Converts a proto type to a wire type.
   */
  public static TieredIdentity.LocalityTier fromProto(alluxio.grpc.LocalityTier localityPTier) {
    return new TieredIdentity.LocalityTier(localityPTier.getTierName(), localityPTier.getValue());
  }

  /**
   * Converts a proto type to a wire type.
   */
  public static MountPointInfo fromProto(alluxio.grpc.MountPointInfo mountPointPInfo) {
    return new MountPointInfo().setUfsUri(mountPointPInfo.getUfsUri())
        .setUfsType(mountPointPInfo.getUfsType())
        .setUfsCapacityBytes(mountPointPInfo.getUfsCapacityBytes())
        .setUfsUsedBytes(mountPointPInfo.getUfsUsedBytes())
        .setReadOnly(mountPointPInfo.getReadOnly())
        .setProperties(mountPointPInfo.getPropertiesMap()).setShared(mountPointPInfo.getShared());
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
   * Converts a proto type to a wire type.
   */
  public static WorkerNetAddress fromProto(alluxio.grpc.WorkerNetAddress workerNetPAddress) {
    WorkerNetAddress workerNetAddress = new WorkerNetAddress();
    workerNetAddress.setHost(workerNetPAddress.getHost());
    workerNetAddress.setRpcPort(workerNetPAddress.getRpcPort());
    workerNetAddress.setDataPort(workerNetPAddress.getDataPort());
    workerNetAddress.setWebPort(workerNetPAddress.getWebPort());
    workerNetAddress.setDomainSocketPath(workerNetPAddress.getDomainSocketPath());
    workerNetAddress.setTieredIdentity(fromProto(workerNetPAddress.getTieredIdentity()));
    return workerNetAddress;
  }

  /**
   * Converts wire type to proto type.
   */
  public static alluxio.grpc.BlockInfo toProto(BlockInfo blockInfo) {
    List<alluxio.grpc.BlockLocation> locations = new ArrayList<>();
    for (BlockLocation location : blockInfo.getLocations()) {
      locations.add(toProto(location));
    }
    return alluxio.grpc.BlockInfo.newBuilder().setBlockId(blockInfo.getBlockId())
        .setLength(blockInfo.getLength()).addAllLocations(locations).build();
  }

  /**
   * Converts wire type to proto type.
   */
  public static alluxio.grpc.BlockLocation toProto(BlockLocation blockLocation) {
    return alluxio.grpc.BlockLocation.newBuilder().setWorkerId(blockLocation.getWorkerId())
        .setWorkerAddress(toProto(blockLocation.getWorkerAddress()))
        .setTierAlias(blockLocation.getTierAlias()).build();
  }

  /**
   * Converts options to proto type.
   */
  public static FileSystemMasterCommonPOptions toProto(CommonOptions options) {
    return FileSystemMasterCommonPOptions.newBuilder()
        .setSyncIntervalMs(options.getSyncIntervalMs()).build();
  }

  /**
   * Converts options to proto type.
   */
  public static CheckConsistencyPOptions toProto(CheckConsistencyOptions options) {
    return CheckConsistencyPOptions.newBuilder()
        .setCommonOptions(toProto(options.getCommonOptions())).build();
  }

  /**
   * Converts options to proto type.
   */
  public static CreateDirectoryPOptions toProto(CreateDirectoryOptions options) {
    CreateDirectoryPOptions.Builder builder = CreateDirectoryPOptions.newBuilder()
        .setAllowExist(options.isAllowExists()).setRecursive(options.isRecursive())
        .setTtl(options.getTtl()).setTtlAction(toProto(options.getTtlAction()))
        .setPersisted(options.isPersisted()).setCommonOptions(toProto(options.getCommonOptions()));
    if (options.getMode() != null) {
      builder.setMode(options.getMode().toShort());
    }
    return builder.build();
  }

  /**
   * Converts options to proto type.
   */
  public static CreateFilePOptions toProto(CreateFileOptions options) {
    CreateFilePOptions.Builder builder =
        CreateFilePOptions.newBuilder().setBlockSizeBytes(options.getBlockSizeBytes())
            .setPersisted(options.isPersisted()).setRecursive(options.isRecursive())
            .setTtl(options.getTtl()).setTtlAction(toProto(options.getTtlAction()))
            .setCommonOptions(toProto(options.getCommonOptions()));
    if (options.getMode() != null) {
      builder.setMode(options.getMode().toShort());
    }
    return builder.build();
  }

  /**
   * Converts options to proto type.
   */
  public static CompleteFilePOptions toProto(CompleteFileOptions options) {
    return CompleteFilePOptions.newBuilder().setUfsLength(options.getUfsLength())
        .setCommonOptions(toProto(options.getCommonOptions())).build();
  }

  /**
   * Converts options to proto type.
   */
  public static DeletePOptions toProto(DeleteOptions options) {
    return DeletePOptions.newBuilder().setRecursive(options.isRecursive())
        .setAlluxioOnly(options.isAlluxioOnly()).setUnchecked(options.isUnchecked())
        .setCommonOptions(toProto(options.getCommonOptions())).build();
  }

  /**
   * Converts options to proto type.
   */
  public static FreePOptions toProto(FreeOptions options) {
    return FreePOptions.newBuilder().setForced(options.isForced())
        .setRecursive(options.isRecursive()).setCommonOptions(toProto(options.getCommonOptions()))
        .build();
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
        .setLength(fileInfo.getLength())
        .setBlockSizeBytes(fileInfo.getBlockSizeBytes())
        .setCreationTimeMs(fileInfo.getCreationTimeMs())
        .setCompleted(fileInfo.isCompleted())
        .setFolder(fileInfo.isFolder())
        .setPinned(fileInfo.isPinned())
        .setCacheable(fileInfo.isCacheable())
        .setPersisted(fileInfo.isPersisted())
        .addAllBlockIds(fileInfo.getBlockIds())
        .setLastModificationTimeMs(fileInfo.getLastModificationTimeMs())
        .setTtl(fileInfo.getTtl())
        .setOwner(fileInfo.getOwner())
        .setGroup(fileInfo.getGroup())
        .setMode(fileInfo.getMode())
        .setPersistenceState(fileInfo.getPersistenceState())
        .setMountPoint(fileInfo.isMountPoint())
        .addAllFileBlockInfos(fileBlockInfos)
        .setTtlAction(GrpcUtils.toProto(fileInfo.getTtlAction()))
        .setMountId(fileInfo.getMountId())
        .setInAlluxioPercentage(fileInfo.getInAlluxioPercentage())
        .setUfsFingerprint(fileInfo.getUfsFingerprint())
        .build();
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
        .setBlockInfo(toProto(fileBlockInfo.getBlockInfo())).setOffset(fileBlockInfo.getOffset())
        .addAllUfsLocations(ufsLocations).addAllUfsStringLocations(fileBlockInfo.getUfsLocations())
        .build();
  }

  /**
   * Converts options to proto type.
   */
  public static GetStatusPOptions toProto(GetStatusOptions options) {
    return GetStatusPOptions.newBuilder()
        .setLoadMetadataType(toProto(options.getLoadMetadataType()))
        .setCommonOptions(toProto(options.getCommonOptions())).build();
  }

  /**
   * Converts options to proto type.
   */
  public static ListStatusPOptions toProto(ListStatusOptions options) {
    return ListStatusPOptions.newBuilder()
        .setLoadDirectChildren(options.getLoadMetadataType() == LoadMetadataType.Once
            || options.getLoadMetadataType() == LoadMetadataType.Always)
        .setLoadMetadataType(toProto(options.getLoadMetadataType()))
        .setCommonOptions(toProto(options.getCommonOptions())).build();
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

  /**
   * Converts wire type to proto type.
   */
  public static alluxio.grpc.LocalityTier toProto(TieredIdentity.LocalityTier localityTier) {
    return alluxio.grpc.LocalityTier.newBuilder().setTierName(localityTier.getTierName())
        .setValue(localityTier.getValue()).build();
  }

  /**
   * Converts wire type to proto type.
   */
  public static alluxio.grpc.MountPointInfo toProto(MountPointInfo info) {
    return alluxio.grpc.MountPointInfo.newBuilder().setUfsUri(info.getUfsUri())
        .setUfsType(info.getUfsType()).setUfsCapacityBytes(info.getUfsCapacityBytes())
        .setReadOnly(info.getReadOnly()).putAllProperties(info.getProperties())
        .setShared(info.getShared()).build();
  }

  /**
   * Converts a wire type to a proto type.
   */
  public static MountPOptions toProto(MountOptions options) {
    MountPOptions.Builder builder = MountPOptions.newBuilder().setReadOnly(options.isReadOnly())
        .setShared(options.isShared()).setCommonOptions(toProto(options.getCommonOptions()));
    if (options.getProperties() != null && !options.getProperties().isEmpty()) {
      builder.putAllProperties(options.getProperties());
    }
    return builder.build();
  }

  /**
   * Converts a wire type to a proto type.
   */
  public static RenamePOptions toProto(RenameOptions options) {
    return RenamePOptions.newBuilder().setCommonOptions(toProto(options.getCommonOptions()))
        .build();
  }

  /**
   * Converts a wire type to a proto type.
   */
  public static SetAttributePOptions toProto(SetAttributeOptions options) {
    SetAttributePOptions.Builder builder =
        SetAttributePOptions.newBuilder().setCommonOptions(toProto(options.getCommonOptions()));
    if (options.getPinned() != null) {
      builder.setPinned(options.getPinned());
    }
    if (options.getTtl() != null) {
      builder.setTtl(options.getTtl());
      builder.setTtlAction(toProto(options.getTtlAction()));
    }
    if (options.getOwner() != null) {
      builder.setOwner(options.getOwner());
    }
    if (options.getGroup() != null) {
      builder.setGroup(options.getGroup());
    }
    if (options.getMode() != null) {
      builder.setMode(options.getMode());
    }
    builder.setRecursive(options.isRecursive());
    return builder.build();
  }
  
  /**
   * Converts wire type to proto type.
   */
  public static alluxio.grpc.TieredIdentity toProto(TieredIdentity tieredIdentity) {
    return alluxio.grpc.TieredIdentity.newBuilder()
        .addAllTiers(
            tieredIdentity.getTiers().stream().map(GrpcUtils::toProto).collect(Collectors.toList()))
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
   * Converts a wire type to a proto type.
   */
  public static UpdateUfsModePOptions toProto(UpdateUfsModeOptions options) {
    UfsMode ufsMode;
    switch (options.getUfsMode()) {
      case NO_ACCESS:
        ufsMode = UfsMode.NoAccess;
        break;
      case READ_ONLY:
        ufsMode = UfsMode.ReadOnly;
        break;
      default:
        ufsMode = UfsMode.ReadWrite;
    }
    return UpdateUfsModePOptions.newBuilder().setUfsMode(ufsMode).build();
  }

  /**
   * Converts wire type to proto type.
   */
  public static alluxio.grpc.WorkerNetAddress toProto(WorkerNetAddress workerNetAddress) {
    alluxio.grpc.WorkerNetAddress.Builder address = alluxio.grpc.WorkerNetAddress.newBuilder()
        .setHost(workerNetAddress.getHost()).setRpcPort(workerNetAddress.getRpcPort())
        .setDataPort(workerNetAddress.getDataPort()).setWebPort(workerNetAddress.getWebPort())
        .setDomainSocketPath(workerNetAddress.getDomainSocketPath());
    if (workerNetAddress.getTieredIdentity() != null) {
      address.setTieredIdentity(toProto(workerNetAddress.getTieredIdentity()));
    }
    return address.build();
  }
}

