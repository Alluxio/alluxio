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
import alluxio.file.options.*;
import alluxio.grpc.*;
import alluxio.master.file.FileSystemMasterOptions;
import alluxio.proto.journal.File;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclAction;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.AclEntryType;
import alluxio.security.authorization.DefaultAccessControlList;
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
import alluxio.wire.WorkerInfo;
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

  private GrpcUtils() {
  } // prevent instantiation

  /**
   * Converts from proto type to options.
   *
   * @param masterOptions the default master options provider
   * @param pOptions      the proto options to convert
   * @return the converted options instance
   */
  public static CheckConsistencyPOptions fromProto(FileSystemMasterOptions masterOptions,
                                                   CheckConsistencyPOptions pOptions) {
    CheckConsistencyPOptions.Builder optionsBuilder = masterOptions.getCheckConsistencyOptions().toBuilder();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        optionsBuilder.setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
      }
    }
    return optionsBuilder.build();
  }

  /**
   * Converts from proto type to options.
   *
   * @param masterOptions the default master options provider
   * @param pOptions      the proto options to convert
   * @return the converted options instance
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
   *
   * @param masterOptions the default master options provider
   * @param pOptions      the proto options to convert
   * @return the converted options instance
   */
  public static FileSystemMasterCommonPOptions fromProtoToProto(FileSystemMasterOptions masterOptions,
                                                                FileSystemMasterCommonPOptions pOptions) {
    FileSystemMasterCommonPOptions.Builder optionsBuilder = toProto(masterOptions.getCommonOptions()).toBuilder();
    if (pOptions != null) {
      if (pOptions.hasSyncIntervalMs()) {
        optionsBuilder.setSyncIntervalMs(pOptions.getSyncIntervalMs());
      }
    }
    return optionsBuilder.build();
  }

  /**
   * Converts from proto type to options.
   *
   * @param masterOptions the default master options provider
   * @param pOptions      the proto options to convert
   * @return the converted options instance
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
   *
   * @param masterOptions the default master options provider
   * @param pOptions      the proto options to convert
   * @return the converted options instance
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
   *
   * @param masterOptions the default master options provider
   * @param pOptions      the proto options to convert
   * @return the converted options instance
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
   *
   * @param masterOptions the default master options provider
   * @param pOptions      the proto options to convert
   * @return the converted options instance
   */
  public static FreePOptions fromProto(FileSystemMasterOptions masterOptions,
                                       FreePOptions pOptions) {
    FreePOptions.Builder optionsBuilder = masterOptions.getFreeOptions().toBuilder();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        optionsBuilder.setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
      }
      optionsBuilder.setForced(pOptions.getForced());
      optionsBuilder.setRecursive(pOptions.getRecursive());
    }
    return optionsBuilder.build();

  }

  /**
   * Converts from proto type to options.
   *
   * @param masterOptions the default master options provider
   * @param pOptions      the proto options to convert
   * @return the converted options instance
   */
  public static GetStatusPOptions fromProto(FileSystemMasterOptions masterOptions,
                                            GetStatusPOptions pOptions) {
    GetStatusPOptions.Builder optionsBuilder = masterOptions.getGetStatusOptions().toBuilder();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        optionsBuilder.setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
      }
      if (pOptions.hasLoadMetadataType()) {
        optionsBuilder.setLoadMetadataType(pOptions.getLoadMetadataType());
      }
    }
    return optionsBuilder.build();
  }

  public static GetStatusPOptions toGetStatusOptions(ExistsPOptions existsOptions) {
    return GetStatusPOptions.newBuilder().setLoadMetadataType(existsOptions.getLoadMetadataType())
            .setCommonOptions(existsOptions.getCommonOptions()).build();
  }

  /**
   * Converts from proto type to options.
   *
   * @param masterOptions the default master options provider
   * @param pOptions      the proto options to convert
   * @return the converted options instance
   */
  public static ListStatusPOptions fromProto(FileSystemMasterOptions masterOptions,
                                             ListStatusPOptions pOptions) {
    ListStatusPOptions.Builder optionsBuilder = masterOptions.getListStatusOptions().toBuilder();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        optionsBuilder.setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
      }
      if (pOptions.hasLoadMetadataType()) {
        optionsBuilder.setLoadMetadataType(pOptions.getLoadMetadataType());
      } else if (pOptions.hasLoadDirectChildren()) {
        optionsBuilder.setLoadMetadataType(LoadMetadataPType.NEVER);
      }
      if (pOptions.hasRecursive()) {
        optionsBuilder.setRecursive(pOptions.getRecursive());
      }
    }
    return optionsBuilder.build();

  }

  /**
   * Converts from proto type to wire type.
   *
   * @param loadMetadataPType the proto representation of loadMetadataType
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
   * @param loadMetadataPType the proto representation of loadMetadataType
   * @return the {@link LoadMetadataType}
   */
  @Nullable
  public static DescendantType fromProto(LoadDescendantPType loadMetadataPType) {
    switch (loadMetadataPType) {
      case NONE:
        return DescendantType.NONE;
      case ONE:
        return DescendantType.ONE;
      case ALL:
        return DescendantType.ALL;
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
   *
   * @param masterOptions the default master options provider
   * @param pOptions      the proto options to convert
   * @return the converted options instance
   */
  public static MountPOptions fromProto(FileSystemMasterOptions masterOptions,
                                        MountPOptions pOptions) {
    MountPOptions.Builder optionsBuilder = masterOptions.getMountOptions().toBuilder();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        optionsBuilder
                .setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
      }
      if (pOptions.hasReadOnly()) {
        optionsBuilder.setReadOnly(pOptions.getReadOnly());
      }
      if (pOptions.getPropertiesMap() != null) {
        optionsBuilder.putAllProperties(pOptions.getPropertiesMap());
      }
      if (pOptions.getShared()) {
        optionsBuilder.setShared(pOptions.getShared());
      }
    }
    return optionsBuilder.build();
  }

  /**
   * Converts from proto type to options.
   *
   * @param masterOptions the default master options provider
   * @param pOptions      the proto options to convert
   * @return the converted options instance
   */
  public static DeletePOptions fromProto(FileSystemMasterOptions masterOptions,
                                         DeletePOptions pOptions) {
    DeletePOptions.Builder optionsBuilder = masterOptions.getDeleteOptions().toBuilder();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        optionsBuilder.setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
      }
      optionsBuilder.setRecursive(pOptions.getRecursive());
      optionsBuilder.setAlluxioOnly(pOptions.getAlluxioOnly());
      optionsBuilder.setUnchecked(pOptions.getUnchecked());
    }
    return optionsBuilder.build();
  }

  /**
   * Converts from proto type to options.
   *
   * @param masterOptions the default master options provider
   * @param pOptions      the proto options to convert
   * @return the converted options instance
   */
  public static RenamePOptions fromProto(FileSystemMasterOptions masterOptions,
                                         RenamePOptions pOptions) {
    RenamePOptions.Builder optionsBuilder = masterOptions.getRenameOptions().toBuilder();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        optionsBuilder.setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
      }
    }
    return optionsBuilder.build();
  }

  /**
   * Converts from proto type to options.
   *
   * @param masterOptions the default master options provider
   * @param pOptions      the proto options to convert
   * @return the converted options instance
   */
  public static SetAclPOptions fromProto(FileSystemMasterOptions masterOptions,
                                         SetAclPOptions pOptions) {
    SetAclPOptions.Builder optionsBuilder = masterOptions.getSetAclOptions().toBuilder();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        optionsBuilder
                .setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
      }
      if (pOptions.hasRecursive()) {
        optionsBuilder.setRecursive(pOptions.getRecursive());
      }
    }
    return optionsBuilder.build();
  }

  /**
   * Converts from proto type to options.
   *
   * @param masterOptions the default master options provider
   * @param pOptions      the proto options to convert
   * @return the converted options instance
   */
  public static SetAttributePOptions fromProto(FileSystemMasterOptions masterOptions,
                                               SetAttributePOptions pOptions) {
    SetAttributePOptions.Builder optionsBuilder = masterOptions.getSetAttributeOptions().toBuilder();
    if (pOptions != null) {
      if (pOptions.hasCommonOptions()) {
        optionsBuilder.setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
      }
      if (pOptions.hasPinned()) {
        optionsBuilder.setPinned(pOptions.getPinned());
      }
      if (pOptions.hasTtl()) {
        optionsBuilder.setTtl(pOptions.getTtl());
      }
      if (pOptions.hasTtlAction()) {
        optionsBuilder.setTtlAction(pOptions.getTtlAction());
      }
      if (pOptions.hasPersisted()) {
        optionsBuilder.setPersisted(pOptions.getPersisted());
      }
      if (pOptions.hasOwner()) {
        optionsBuilder.setOwner(pOptions.getOwner());
      }
      if (pOptions.hasGroup()) {
        optionsBuilder.setGroup(pOptions.getGroup());
      }
      if (pOptions.hasMode()) {
        optionsBuilder.setMode((short) pOptions.getMode());
      }
      optionsBuilder.setRecursive(pOptions.getRecursive());
    }
    return optionsBuilder.build();
  }

  /**
   * @param pAcl the proto representation
   * @return the {@link AccessControlList} instance created from the proto representation
   */
  public static AccessControlList fromProto(PAcl pAcl) {
    AccessControlList acl;

    if (pAcl.getIsDefault()) {
      acl = new DefaultAccessControlList();
      ((DefaultAccessControlList) acl).setEmpty(pAcl.getIsDefaultEmpty());
    } else {
      acl = new AccessControlList();
    }

    acl.setOwningUser(pAcl.getOwner());
    acl.setOwningGroup(pAcl.getOwningGroup());
    acl.setMode((short) pAcl.getMode());

    if (pAcl.getEntriesCount() > 0) {
      for (PAclEntry tEntry : pAcl.getEntriesList()) {
        acl.setEntry(fromProto(tEntry));
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
   *
   * @param blockPLocation the proto type to convert
   * @return the converted wire type
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
   *
   * @param blockPInfo the proto type to convert
   * @return the converted wire type
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
            .setAcl(pInfo.hasAcl() ? (fromProto(pInfo.getAcl()))
                    : AccessControlList.EMPTY_ACL)
            .setDefaultAcl(
                    pInfo.hasDefaultAcl() ? ((DefaultAccessControlList) fromProto(pInfo.getDefaultAcl()))
                            : DefaultAccessControlList.EMPTY_DEFAULT_ACL);
  }

  /**
   * Converts a proto type to a wire type.
   *
   * @param fileBlockPInfo the proto type to convert
   * @return the converted wire type
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
   *
   * @param tieredPIdentity the proto type to convert
   * @return the converted wire type
   */
  public static TieredIdentity fromProto(alluxio.grpc.TieredIdentity tieredPIdentity) {
    return new TieredIdentity(tieredPIdentity.getTiersList().stream().map(GrpcUtils::fromProto)
            .collect(Collectors.toList()));
  }

  /**
   * Converts a proto type to a wire type.
   *
   * @param localityPTier the proto type to convert
   * @return the converted wire type
   */
  public static TieredIdentity.LocalityTier fromProto(alluxio.grpc.LocalityTier localityPTier) {
    return new TieredIdentity.LocalityTier(localityPTier.getTierName(), localityPTier.getValue());
  }

  /**
   * Converts a proto type to a wire type.
   *
   * @param mountPointPInfo the proto type to convert
   * @return the converted wire type
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
   * @param tTtlAction {@link TtlAction}
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
   *
   * @param workerInfo the proto type to convert
   * @return the converted wire type
   */
  public static WorkerInfo fromProto(alluxio.grpc.WorkerInfo workerInfo) {
    return new WorkerInfo()
            .setAddress(fromProto(workerInfo.getAddress()))
            .setCapacityBytes(workerInfo.getCapacityBytes())
            .setCapacityBytesOnTiers(workerInfo.getCapacityBytesOnTiers())
            .setId(workerInfo.getId())
            .setLastContactSec(workerInfo.getLastContactSec())
            .setStartTimeMs(workerInfo.getStartTimeMs())
            .setState(workerInfo.getState())
            .setUsedBytes(workerInfo.getUsedBytes())
            .setUsedBytesOnTiers(workerInfo.getUsedBytesOnTiersMap());
  }

  /**
   * Converts a proto type to a wire type.
   *
   * @param workerNetPAddress the proto type to convert
   * @return the converted wire type
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
   * @param acl the access control list to convert
   * @return the proto representation of this object
   */
  public static PAcl toProto(AccessControlList acl) {
    PAcl.Builder pAcl = PAcl.newBuilder();
    pAcl.setOwner(acl.getOwningUser());
    pAcl.setOwningGroup(acl.getOwningGroup());
    pAcl.setMode(acl.getMode());
    if (acl.hasExtended()) {
      for (AclEntry entry : acl.getExtendedEntries().getEntries()) {
        pAcl.addEntries(toProto(entry));
      }
    }
    pAcl.setIsDefault(false);
    return pAcl.build();
  }

  /**
   * @param defaultAcl the default access control list to convert
   * @return the proto representation of default acl object
   */
  public static PAcl toProto(DefaultAccessControlList defaultAcl) {
    PAcl.Builder pAcl = PAcl.newBuilder(toProto((AccessControlList) defaultAcl));
    pAcl.setIsDefault(true);
    pAcl.setIsDefaultEmpty(defaultAcl.isEmpty());
    return pAcl.build();
  }

  /**
   * @param action the acl action to convert
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
   * @param aclEntry the acl entry to convert
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
   * @param aclEntryType the acl entry type to convert
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
   *
   * @param blockInfo the wire type to convert
   * @return the converted proto type
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
   *
   * @param blockLocation the wire type to convert
   * @return the converted proto type
   */
  public static alluxio.grpc.BlockLocation toProto(BlockLocation blockLocation) {
    return alluxio.grpc.BlockLocation.newBuilder().setWorkerId(blockLocation.getWorkerId())
            .setWorkerAddress(toProto(blockLocation.getWorkerAddress()))
            .setTierAlias(blockLocation.getTierAlias()).build();
  }

  /**
   * Converts options to proto type.
   *
   * @param options the options type to convert
   * @return the converted proto type
   */
  public static FileSystemMasterCommonPOptions toProto(CommonOptions options) {
    return FileSystemMasterCommonPOptions.newBuilder()
            .setSyncIntervalMs(options.getSyncIntervalMs())
            .setTtl(options.getTtl())
            .setTtlAction(toProto(options.getTtlAction()))
            .build();
  }

  /**
   * Converts options to proto type.
   *
   * @param options the options type to convert
   * @return the converted proto type
   */
  public static CreateDirectoryPOptions toProto(CreateDirectoryOptions options) {
    CreateDirectoryPOptions.Builder builder = CreateDirectoryPOptions.newBuilder()
            .setAllowExist(options.isAllowExists())
            .setRecursive(options.isRecursive())
            .setPersisted(options.isPersisted())
            .setCommonOptions(toProto(options.getCommonOptions()));
    if (options.getMode() != null) {
      builder.setMode(options.getMode().toShort());
    }
    return builder.build();
  }

  /**
   * Converts options to proto type.
   *
   * @param options the options type to convert
   * @return the converted proto type
   */
  public static CreateFilePOptions toProto(CreateFileOptions options) {
    CreateFilePOptions.Builder builder = CreateFilePOptions.newBuilder()
            .setBlockSizeBytes(options.getBlockSizeBytes())
            .setPersisted(options.isPersisted())
            .setRecursive(options.isRecursive())
            .setCommonOptions(toProto(options.getCommonOptions()));
    if (options.getMode() != null) {
      builder.setMode(options.getMode().toShort());
    }
    return builder.build();
  }

  /**
   * Converts options to proto type.
   *
   * @param options the options type to convert
   * @return the converted proto type
   */
  public static CompleteFilePOptions toProto(CompleteFileOptions options) {
    return CompleteFilePOptions.newBuilder()
            .setUfsLength(options.getUfsLength())
            .setCommonOptions(toProto(options.getCommonOptions()))
            .build();
  }

  /**
   * Converts options to proto type.
   *
   * @param options the options type to convert
   * @return the converted proto type
   */
  public static CreateUfsFilePOptions toProto(CreateUfsFileOptions options) {
    CreateUfsFilePOptions.Builder builder = CreateUfsFilePOptions.newBuilder();
    if (!options.getOwner().isEmpty()) {
      builder.setOwner(options.getOwner());
    }
    if (!options.getGroup().isEmpty()) {
      builder.setGroup(options.getGroup());
    }
    if (options.getMode() != null && options.getMode().toShort() != Constants.INVALID_MODE) {
      builder.setMode(options.getMode().toShort());
    }
    return builder.build();
  }

  /**
   * Converts options to proto type.
   *
   * @param options the options type to convert
   * @return the converted proto type
   */
  public static CompleteUfsFilePOptions toProto(CompleteUfsFileOptions options) {
    CompleteUfsFilePOptions.Builder builder = CompleteUfsFilePOptions.newBuilder();
    if (!options.getOwner().isEmpty()) {
      builder.setOwner(options.getOwner());
    }
    if (!options.getGroup().isEmpty()) {
      builder.setGroup(options.getGroup());
    }
    if (options.getMode() != null && options.getMode().toShort() != Constants.INVALID_MODE) {
      builder.setMode(options.getMode().toShort());
    }
    return builder.build();
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
    alluxio.grpc.FileInfo.Builder builder = alluxio.grpc.FileInfo.newBuilder()
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
            .setUfsFingerprint(fileInfo.getUfsFingerprint());
    if (!fileInfo.getAcl().equals(AccessControlList.EMPTY_ACL)) {
      builder.setAcl(toProto(fileInfo.getAcl()));
    }
    if (!fileInfo.getDefaultAcl().equals(DefaultAccessControlList.EMPTY_DEFAULT_ACL)) {
      builder.setDefaultAcl(toProto(fileInfo.getDefaultAcl()));
    }
    return builder.build();
  }

  /**
   * Converts wire type to proto type.
   *
   * @param fileBlockInfo the wire representation to convert
   * @return converted proto representation
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
   *
   * @param loadMetadataType the {@link LoadMetadataType}
   * @return the proto representation of this enum
   */
  public static LoadMetadataPType toProto(LoadMetadataType loadMetadataType) {
    return LoadMetadataPType.forNumber(loadMetadataType.getValue());
  }

  /**
   * Converts options to proto type.
   *
   * @param loadDescendantType the {@link DescendantType}
   * @return the proto representation of this enum
   */
  public static LoadDescendantPType toProto(DescendantType loadDescendantType) {
    return LoadDescendantPType.valueOf(loadDescendantType.name());
  }

  /**
   * Converts wire type to proto type.
   *
   * @param localityTier the wire representation to convert
   * @return converted proto representation
   */
  public static alluxio.grpc.LocalityTier toProto(TieredIdentity.LocalityTier localityTier) {
    return alluxio.grpc.LocalityTier.newBuilder().setTierName(localityTier.getTierName())
            .setValue(localityTier.getValue()).build();
  }

  /**
   * Converts wire type to proto type.
   *
   * @param info the wire representation to convert
   * @return converted proto representation
   */
  public static alluxio.grpc.MountPointInfo toProto(MountPointInfo info) {
    return alluxio.grpc.MountPointInfo.newBuilder().setUfsUri(info.getUfsUri())
            .setUfsType(info.getUfsType()).setUfsCapacityBytes(info.getUfsCapacityBytes())
            .setReadOnly(info.getReadOnly()).putAllProperties(info.getProperties())
            .setShared(info.getShared()).build();
  }

  public static MountPOptions fromMountEntry(File.AddMountPointEntry mountEntryPoint) {
    MountPOptions.Builder optionsBuilder = MountPOptions.newBuilder();
    if (mountEntryPoint != null) {
      if (mountEntryPoint.hasReadOnly()) {
        optionsBuilder.setReadOnly(mountEntryPoint.getReadOnly());
      }
      for (File.StringPairEntry entry : mountEntryPoint.getPropertiesList()) {
        optionsBuilder.putProperties(entry.getKey(), entry.getValue());
      }
      if (mountEntryPoint.hasShared()) {
        optionsBuilder.setShared(mountEntryPoint.getShared());
      }
    }
    return optionsBuilder.build();
  }

  /**
   * Converts wire type to proto type.
   *
   * @param aclAction the wire representation to convert
   * @return the converted proto representation
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
   * Converts wire type to proto type.
   *
   * @param tieredIdentity the wire representation to convert
   * @return the converted proto representation
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
   * @return {@link TtlAction} equivalent
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
   *
   * @param workerInfo the wire representation to convert
   * @return the converted proto representation
   */
  public static alluxio.grpc.WorkerInfo toProto(WorkerInfo workerInfo) {
    return alluxio.grpc.WorkerInfo.newBuilder()
            .setId(workerInfo.getId())
            .setAddress(toProto(workerInfo.getAddress()))
            .setLastContactSec(workerInfo.getLastContactSec())
            .setState(workerInfo.getState())
            .setCapacityBytes(workerInfo.getCapacityBytes())
            .setUsedBytes(workerInfo.getUsedBytes())
            .setStartTimeMs(workerInfo.getStartTimeMs())
            .putAllCapacityBytesOnTiers(workerInfo.getCapacityBytesOnTiers())
            .putAllUsedBytesOnTiers(workerInfo.getUsedBytesOnTiers())
            .build();
  }

  /**
   * Converts wire type to proto type.
   *
   * @param workerNetAddress the wire representation to convert
   * @return the converted proto representation
   */
  public static alluxio.grpc.WorkerNetAddress toProto(WorkerNetAddress workerNetAddress) {
    alluxio.grpc.WorkerNetAddress.Builder address = alluxio.grpc.WorkerNetAddress.newBuilder()
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
   * @return true if the read type imposes caching, false otherwise
   */
  public static boolean isCache(ReadPType readType) {
    return readType == ReadPType.READ_CACHE || readType == ReadPType.READ_CACHE_PROMOTE;
  }

  /**
   * @return true if the read type imposes promoting in cache, false otherwise
   */
  public static boolean isPromote(ReadPType readType) {
    return readType == ReadPType.READ_CACHE_PROMOTE;
  }

  /**
   * @return true if by this write type data will be persisted <em>asynchronously</em> to under
   *         storage (e.g., {@literal WritePType.WRITE_ASYNC_THROUGH}), false otherwise
   */
  public static boolean isWriteTypeAsync(WritePType writeType) {
    return writeType == WritePType.WRITE_ASYNC_THROUGH;
  }

  /**
   * @return true if by this write type data will be cached in Alluxio space (e.g.,
   *         {@literal WritePType.WRITE_MUST_CACHE}, {@literal WritePType.WRITE_CACHE_THROUGH},
   *         {@literal WritePType.WRITE_TRY_CACHE}, or {@literal WritePType.WRITE_ASYNC_THROUGH}),
   *         false otherwise
   */
  public static boolean isWriteTypeCache(WritePType writeType) {
    return (writeType == WritePType.WRITE_MUST_CACHE)
        || (writeType == WritePType.WRITE_CACHE_THROUGH)
        || (writeType == WritePType.WRITE_TRY_CACHE)
        || (writeType == WritePType.WRITE_ASYNC_THROUGH);
  }

  /**
   * @return true if by this write type data will be persisted <em>synchronously</em> to under
   *         storage (e.g., {@literal WritePType.WRITE_CACHE_THROUGH} or
   *         {@literal WritePType.WRITE_THROUGH}), false otherwise
   */
  public static boolean isWriteTypeThrough(WritePType writeType) {
    return (writeType == WritePType.WRITE_CACHE_THROUGH) || (writeType == WritePType.WRITE_THROUGH);
  }
}