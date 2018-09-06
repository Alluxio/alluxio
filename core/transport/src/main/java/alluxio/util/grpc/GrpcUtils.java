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

import static alluxio.util.StreamUtils.map;

import alluxio.Constants;
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
import alluxio.file.options.SetAclOptions;
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
import alluxio.grpc.PAcl;
import alluxio.grpc.PAclAction;
import alluxio.grpc.PAclEntry;
import alluxio.grpc.PAclEntryType;
import alluxio.grpc.PSetAclAction;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.UfsMode;
import alluxio.grpc.UpdateUfsModePOptions;
import alluxio.master.file.FileSystemMasterOptions;
import alluxio.security.authorization.AclAction;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.AclEntryType;
import alluxio.security.authorization.Mode;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SetAclAction;
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
      // mOwner = SecurityUtils.getOwnerFromProtoClient();
      // mGroup = SecurityUtils.getGroupFromProtoClient();
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
      // mOwner = SecurityUtils.getOwnerFromProtoClient();
      // mGroup = SecurityUtils.getGroupFromProtoClient();
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
      // TODO(adit): update recursive
    }
    return options;

  }

  /**
   * Converts from proto type to wire type.
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
   * Converts from proto type to wire type.
   *
   * @param pSetAclAction {@link PSetAclAction}
   * @return {@link SetAclAction} equivalent
   */
  public static SetAclAction fromProto(PSetAclAction pSetAclAction) {
    if (pSetAclAction == null) {
      throw new IllegalStateException("Null proto set acl action.");
    }
    switch (pSetAclAction) {
      case Replace:
        return SetAclAction.REPLACE;
      case Modify:
        return SetAclAction.MODIFY;
      case Remove:
        return SetAclAction.REMOVE;
      case RemoveAll:
        return SetAclAction.REMOVE_ALL;
      case RemoveDefault:
        return SetAclAction.REMOVE_DEFAULT;
      default:
        throw new IllegalStateException("Unrecognized proto set acl action: " + pSetAclAction);
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
  public static SetAclOptions fromProto(FileSystemMasterOptions masterOptions, SetAclPOptions pOptions) {
    SetAclOptions options = masterOptions.getSetAclOptions();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        options.setCommonOptions(fromProto(masterOptions, pOptions.getCommonOptions()));
      }
      if (pOptions.hasRecursive()) {
        options.setRecursive(pOptions.getRecursive());
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
   * @param tAcl the thrift representation
   * @return the {@link AccessControlList} instance created from the thrift representation
   */
  public static AccessControlList fromThrift(TAcl tAcl) {
    AccessControlList acl;

    if (tAcl.isIsDefault()) {
      acl = new DefaultAccessControlList();
      ((DefaultAccessControlList) acl).setEmpty(tAcl.isIsDefaultEmpty());
    } else {
      acl = new AccessControlList();
    }

    acl.setOwningUser(tAcl.getOwner());
    acl.setOwningGroup(tAcl.getOwningGroup());
    acl.setMode(tAcl.getMode());

    if (tAcl.isSetEntries()) {
      for (TAclEntry tEntry : tAcl.getEntries()) {
        acl.setEntry(AclEntry.fromThrift(tEntry));
      }
    }

    return acl;
  }

  /**
   * @param pAclEntry the proto representation
   * @return the {@link AclEntry} instance created from the proto representation
   */
  public static AclEntry fromProto(PAclEntry pAclEntry) {
    AclEntry.Builder builder = new AclEntry.Builder();
    builder.setType(fromProto(pAclEntry.getType()));
    builder.setSubject(pAclEntry.getSubject());
    builder.setIsDefault(pAclEntry.getIsDefault());
    if (pAclEntry.getActionsCount() > 0) {
      for (PAclAction pAclAction : pAclEntry.getActionsList()) {
        builder.addAction(fromProto(pAclAction));
      }
    }
    return builder.build();
  }

  /**
   * @param pAction the proto representation
   * @return the {@link AclAction} created from the proto representation
   */
  public static AclAction fromProto(PAclAction pAction) {
    switch (pAction) {
      case Read:
        return AclAction.READ;
      case Write:
        return AclAction.WRITE;
      case Execute:
        return AclAction.EXECUTE;
      default:
        throw new IllegalStateException("Unknown TAclACtion: " + pAction);
    }
  }

  /**
   * @param pAclEntryType the proto representation
   * @return the {@link AclEntryType} created from the proto representation
   */
  public static AclEntryType fromProto(PAclEntryType pAclEntryType) {
    switch (pAclEntryType) {
      case Owner:
        return AclEntryType.OWNING_USER;
      case NamedUser:
        return AclEntryType.NAMED_USER;
      case OwningGroup:
        return AclEntryType.OWNING_GROUP;
      case NamedGroup:
        return AclEntryType.NAMED_GROUP;
      case Mask:
        return AclEntryType.MASK;
      case Other:
        return AclEntryType.OTHER;
      default:
        throw new IllegalStateException("Unknown TAclEntryType: " + pAclEntryType);
    }
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
    blockInfo.setLocations(map(GrpcUtils::fromProto, blockPInfo.getLocationsList()));
    return blockInfo;
  }

  /**
   * Converts a proto type to a wire type.
   *
   * @param pInfo the proto representation of a file information
   * @return wire representation of the file information
   */
  public static FileInfo fromProto(alluxio.grpc.FileInfo pInfo) {
    return new FileInfo()
        .setFileId(pInfo.getFileId())
        .setName(pInfo.getName())
        .setPath(pInfo.getPath())
        .setUfsPath(pInfo.getUfsPath())
        .setLength(pInfo.getLength())
        .setBlockSizeBytes(pInfo.getBlockSizeBytes())
        .setCreationTimeMs(pInfo.getCreationTimeMs())
        .setCompleted(pInfo.getCompleted())
        .setFolder(pInfo.getFolder())
        .setPinned(pInfo.getPinned())
        .setCacheable(pInfo.getCacheable())
        .setPersisted(pInfo.getPersisted())
        .setBlockIds(pInfo.getBlockIdsList())
        .setLastModificationTimeMs(pInfo.getLastModificationTimeMs())
        .setTtl(pInfo.getTtl())
        .setTtlAction(fromProto(pInfo.getTtlAction()))
        .setOwner(pInfo.getOwner())
        .setGroup(pInfo.getGroup())
        .setMode(pInfo.getMode())
        .setPersistenceState(pInfo.getPersistenceState())
        .setMountPoint(pInfo.getMountPoint())
        .setFileBlockInfos(map(GrpcUtils::fromProto, pInfo.getFileBlockInfosList()))
        .setMountId(pInfo.getMountId())
        .setInAlluxioPercentage(pInfo.getInAlluxioPercentage())
        .setUfsFingerprint(pInfo.hasUfsFingerprint() ? pInfo.getUfsFingerprint()
            : Constants.INVALID_UFS_FINGERPRINT)
        .setAcl(pInfo.hasAcl() ? (AccessControlList.fromThrift(info.getAcl()))
            : AccessControlList.EMPTY_ACL)
        .setDefaultAcl(
            pInfo.hasDefaultAcl() ? ((DefaultAccessControlList) fromProto(pInfo.getDefaultAcl()))
                : DefaultAccessControlList.EMPTY_DEFAULT_ACL);
  }

  /**
   * Converts a proto type to a wire type.
   */
  public static FileBlockInfo fromProto(alluxio.grpc.FileBlockInfo fileBlockPInfo) {
    return new FileBlockInfo()
        .setBlockInfo(fromProto(fileBlockPInfo.getBlockInfo()))
        .setOffset(fileBlockPInfo.getOffset())
        .setUfsLocations(
            fileBlockPInfo.getUfsLocationsCount() > 0 ? fileBlockPInfo.getUfsStringLocationsList()
                : map(addr -> HostAndPort.fromParts(addr.getHost(), addr.getDataPort()).toString(),
                    fileBlockPInfo.getUfsLocationsList()));
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
   * @return the thrift representation of this object
   */
  public TAcl toThrift(AccessControlList list) {
    TAcl tAcl = new TAcl();
    tAcl.setOwner(getOwningUser());
    tAcl.setOwningGroup(getOwningGroup());
    tAcl.setMode(mMode);
    if (hasExtended()) {
      for (AclEntry entry : mExtendedEntries.getEntries()) {
        tAcl.addToEntries(entry.toThrift());
      }
    }
    tAcl.setIsDefault(false);
    return tAcl;
  }

  /**
   * @return the thrift representation of this object
   */
  @Override
  public TAcl toThrift(DefaultAccessControlList list) {
    TAcl tAcl = super.toThrift();
    tAcl.setIsDefault(true);
    tAcl.setIsDefaultEmpty(isEmpty());
    return tAcl;
  }

  /**
   * @return the proto representation of this enum
   */
  public static PAclAction toProto(AclAction action) {
    switch (action) {
      case READ:
        return PAclAction.Read;
      case WRITE:
        return PAclAction.Write;
      case EXECUTE:
        return PAclAction.Execute;
      default:
        throw new IllegalStateException("Unknown acl action: " + action);
    }
  }

  /**
   * @return the proto representation of AclEntry instance
   */
  public static PAclEntry toProto(AclEntry aclEntry) {
    PAclEntry.Builder pAclEntry = PAclEntry.newBuilder();
    pAclEntry.setType(toProto(aclEntry.getType()));
    pAclEntry.setSubject(aclEntry.getSubject());
    pAclEntry.setIsDefault(aclEntry.isDefault());
    for (AclAction action : aclEntry.getActions().getActions()) {
      pAclEntry.addActions(toProto(action));
    }
    return pAclEntry.build();
  }

  /**
   * @return the proto representation of this enum
   */
  public static PAclEntryType toProto(AclEntryType aclEntryType) {
    switch (aclEntryType) {
      case OWNING_USER:
        return PAclEntryType.Owner;
      case NAMED_USER:
        return PAclEntryType.NamedUser;
      case OWNING_GROUP:
        return PAclEntryType.OwningGroup;
      case NAMED_GROUP:
        return PAclEntryType.NamedGroup;
      case MASK:
        return PAclEntryType.Mask;
      case OTHER:
        return PAclEntryType.Other;
      default:
        throw new IllegalStateException("Unknown AclEntryType: " + aclEntryType);
    }
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
    // TODO(adit): update options
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
    PAcl pAcl = fileInfo.getAcl().equals(AccessControlList.EMPTY_ACL) ? null : toProto(fileInfo.getAcl());
    PAcl pDefaultAcl = mDefaultAcl.equals(DefaultAccessControlList.EMPTY_DEFAULT_ACL)
        ? null : mDefaultAcl.toThrift();
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
        .setAcl(pAcl)
        .setDefaultAcl(pDefaultAcl)
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
    // TODO(adit): update recursive
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
   * Converts wire type to proto type.
   */
  public static alluxio.grpc.PSetAclAction toProto(SetAclAction aclAction) {
    switch (aclAction) {
      case REPLACE:
        return PSetAclAction.Replace;
      case MODIFY:
        return PSetAclAction.Modify;
      case REMOVE:
        return PSetAclAction.Remove;
      case REMOVE_ALL:
        return PSetAclAction.RemoveAll;
      case REMOVE_DEFAULT:
        return PSetAclAction.RemoveDefault;
      default:
        throw new IllegalStateException("Unrecognized set acl action: " + aclAction);
    }
  }

  /**
   * Converts a wire type to a proto type.
   */
  public static SetAclPOptions toProto(SetAclOptions options) {
    return SetAclPOptions.newBuilder()
        .setRecursive(options.getRecursive())
        .setCommonOptions(toProto(options.getCommonOptions()))
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

  // TODO(adit): ACL conversions for createpath
}

