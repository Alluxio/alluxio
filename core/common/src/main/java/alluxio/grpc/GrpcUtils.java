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

package alluxio.grpc;

import static alluxio.util.StreamUtils.map;

import alluxio.Constants;
import alluxio.file.options.DescendantType;
import alluxio.proto.journal.File;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclAction;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.AclEntryType;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.CommandType;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.FileSystemCommand;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.MountPointInfo;
import alluxio.wire.PersistFile;
import alluxio.wire.TieredIdentity;
import alluxio.wire.UfsInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for conversion between wire types and grpc types.
 */
@ThreadSafe
public final class GrpcUtils {
  private GrpcUtils() {} // prevent instantiation

  /**
   * Converts from proto type to options.
   *
   * @param existsOptions the proto options to convert
   * @return the converted proto options
   */
  public static GetStatusPOptions toGetStatusOptions(ExistsPOptions existsOptions) {
    GetStatusPOptions.Builder getStatusOptionsBuilder = GetStatusPOptions.newBuilder();
    if (existsOptions.hasCommonOptions()) {
      getStatusOptionsBuilder.setCommonOptions(existsOptions.getCommonOptions());
    }
    if (existsOptions.hasLoadMetadataType()) {
      getStatusOptionsBuilder.setLoadMetadataType(existsOptions.getLoadMetadataType());
    }
    return getStatusOptionsBuilder.build();
  }

  /**
   * Creates mount proto options from {@link File.AddMountPointEntry}.
   * @param mountEntryPoint mount point entry
   * @return created mount proto options
   */
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
    blockLocation.setMediumType(blockPLocation.getMediumType());
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
   * @param pDescendantType the proto representation of a descendant type
   * @return the wire representation of the descendant type
   */
  public static DescendantType fromProto(alluxio.grpc.LoadDescendantPType pDescendantType) {
    switch (pDescendantType) {
      case NONE:
        return DescendantType.NONE;
      case ONE:
        return DescendantType.ONE;
      case ALL:
        return DescendantType.ALL;
      default:
        throw new IllegalStateException("Unknown DescendantType: " + pDescendantType);
    }
  }

  /**
   * Converts a proto type to a wire type.
   *
   * @param pInfo the proto representation of a file information
   * @return wire representation of the file information
   */
  public static FileInfo fromProto(alluxio.grpc.FileInfo pInfo) {
    FileInfo fileInfo = new FileInfo().setFileId(pInfo.getFileId()).setName(pInfo.getName())
        .setPath(pInfo.getPath()).setUfsPath(pInfo.getUfsPath()).setLength(pInfo.getLength())
        .setBlockSizeBytes(pInfo.getBlockSizeBytes()).setCreationTimeMs(pInfo.getCreationTimeMs())
        .setCompleted(pInfo.getCompleted()).setFolder(pInfo.getFolder())
        .setPinned(pInfo.getPinned()).setCacheable(pInfo.getCacheable())
        .setMediumTypes(ImmutableSet.copyOf(pInfo.getMediumTypeList()))
        .setPersisted(pInfo.getPersisted()).setBlockIds(pInfo.getBlockIdsList())
        .setLastModificationTimeMs(pInfo.getLastModificationTimeMs()).setTtl(pInfo.getTtl())
        .setLastAccessTimeMs(pInfo.getLastAccessTimeMs())
        .setTtlAction(pInfo.getTtlAction()).setOwner(pInfo.getOwner())
        .setGroup(pInfo.getGroup()).setMode(pInfo.getMode())
        .setPersistenceState(pInfo.getPersistenceState()).setMountPoint(pInfo.getMountPoint())
        .setFileBlockInfos(map(GrpcUtils::fromProto, pInfo.getFileBlockInfosList()))
        .setMountId(pInfo.getMountId()).setInAlluxioPercentage(pInfo.getInAlluxioPercentage())
        .setInMemoryPercentage(pInfo.getInMemoryPercentage())
        .setUfsFingerprint(pInfo.hasUfsFingerprint() ? pInfo.getUfsFingerprint()
            : Constants.INVALID_UFS_FINGERPRINT)
        .setAcl(pInfo.hasAcl() ? (fromProto(pInfo.getAcl())) : AccessControlList.EMPTY_ACL)
        .setDefaultAcl(
            pInfo.hasDefaultAcl() ? ((DefaultAccessControlList) fromProto(pInfo.getDefaultAcl()))
                : DefaultAccessControlList.EMPTY_DEFAULT_ACL)
        .setReplicationMax(pInfo.getReplicationMax()).setReplicationMin(pInfo.getReplicationMin())
        .setXAttr(pInfo.getXattrMap().entrySet().stream().collect(Collectors.toMap(Map
            .Entry::getKey, e -> e.getValue().toByteArray())));
    return fileInfo;
  }

  /**
   * Converts a proto type to a wire type.
   *
   * @param fileBlockPInfo the proto type to convert
   * @return the converted wire type
   */
  public static FileBlockInfo fromProto(alluxio.grpc.FileBlockInfo fileBlockPInfo) {
    return new FileBlockInfo().setBlockInfo(fromProto(fileBlockPInfo.getBlockInfo()))
        .setOffset(fileBlockPInfo.getOffset()).setUfsLocations(
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
    return new TieredIdentity.LocalityTier(localityPTier.getTierName(),
        localityPTier.hasValue() ? localityPTier.getValue() : null);
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
        .setProperties(mountPointPInfo.getPropertiesMap())
        .setMountId(mountPointPInfo.getMountId())
        .setShared(mountPointPInfo.getShared());
  }

  /**
   * Converts a proto type to a wire type.
   *
   * @param workerInfo the proto type to convert
   * @return the converted wire type
   */
  public static WorkerInfo fromProto(alluxio.grpc.WorkerInfo workerInfo) {
    return new WorkerInfo().setAddress(fromProto(workerInfo.getAddress()))
        .setCapacityBytes(workerInfo.getCapacityBytes())
        .setCapacityBytesOnTiers(workerInfo.getCapacityBytesOnTiers()).setId(workerInfo.getId())
        .setLastContactSec(workerInfo.getLastContactSec())
        .setStartTimeMs(workerInfo.getStartTimeMs()).setState(workerInfo.getState())
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
    workerNetAddress.setContainerHost(workerNetPAddress.getContainerHost());
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
        .setTierAlias(blockLocation.getTierAlias())
        .setMediumType(blockLocation.getMediumType())
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
    alluxio.grpc.FileInfo.Builder builder = alluxio.grpc.FileInfo.newBuilder()
        .setFileId(fileInfo.getFileId()).setName(fileInfo.getName()).setPath(fileInfo.getPath())
        .setUfsPath(fileInfo.getUfsPath()).setLength(fileInfo.getLength())
        .setBlockSizeBytes(fileInfo.getBlockSizeBytes())
        .setCreationTimeMs(fileInfo.getCreationTimeMs()).setCompleted(fileInfo.isCompleted())
        .setFolder(fileInfo.isFolder()).setPinned(fileInfo.isPinned())
        .setCacheable(fileInfo.isCacheable()).setPersisted(fileInfo.isPersisted())
        .addAllBlockIds(fileInfo.getBlockIds())
        .setLastModificationTimeMs(fileInfo.getLastModificationTimeMs()).setTtl(fileInfo.getTtl())
        .setLastAccessTimeMs(fileInfo.getLastAccessTimeMs())
        .setOwner(fileInfo.getOwner()).setGroup(fileInfo.getGroup()).setMode(fileInfo.getMode())
        .setPersistenceState(fileInfo.getPersistenceState()).setMountPoint(fileInfo.isMountPoint())
        .addAllFileBlockInfos(fileBlockInfos)
        .setTtlAction(fileInfo.getTtlAction()).setMountId(fileInfo.getMountId())
        .setInAlluxioPercentage(fileInfo.getInAlluxioPercentage())
        .setInMemoryPercentage(fileInfo.getInMemoryPercentage())
        .setUfsFingerprint(fileInfo.getUfsFingerprint())
        .setReplicationMax(fileInfo.getReplicationMax())
        .setReplicationMin(fileInfo.getReplicationMin());

    if (!fileInfo.getAcl().equals(AccessControlList.EMPTY_ACL)) {
      builder.setAcl(toProto(fileInfo.getAcl()));
    }
    if (!fileInfo.getDefaultAcl().equals(DefaultAccessControlList.EMPTY_DEFAULT_ACL)) {
      builder.setDefaultAcl(toProto(fileInfo.getDefaultAcl()));
    }
    if (fileInfo.getXAttr() != null) {
      for (Map.Entry<String, byte[]> entry : fileInfo.getXAttr().entrySet()) {
        builder.putXattr(entry.getKey(), ByteString.copyFrom(entry.getValue()));
      }
    }
    if (!fileInfo.getMediumTypes().isEmpty()) {
      builder.addAllMediumType(fileInfo.getMediumTypes());
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
      ufsLocations.add(alluxio.grpc.WorkerNetAddress.newBuilder().setHost(address.getHost())
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
    alluxio.grpc.LocalityTier.Builder tier =
        alluxio.grpc.LocalityTier.newBuilder().setTierName(localityTier.getTierName());
    if (localityTier.getValue() != null) {
      tier.setValue(localityTier.getValue());
    }
    return tier.build();
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
        .setShared(info.getShared())
        .setMountId(info.getMountId())
        .setUfsUsedBytes(info.getUfsUsedBytes())
        .build();
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
   * @param workerInfo the wire representation to convert
   * @return the converted proto representation
   */
  public static alluxio.grpc.WorkerInfo toProto(WorkerInfo workerInfo) {
    return alluxio.grpc.WorkerInfo.newBuilder().setId(workerInfo.getId())
        .setAddress(toProto(workerInfo.getAddress()))
        .setLastContactSec(workerInfo.getLastContactSec()).setState(workerInfo.getState())
        .setCapacityBytes(workerInfo.getCapacityBytes()).setUsedBytes(workerInfo.getUsedBytes())
        .setStartTimeMs(workerInfo.getStartTimeMs())
        .putAllCapacityBytesOnTiers(workerInfo.getCapacityBytesOnTiers())
        .putAllUsedBytesOnTiers(workerInfo.getUsedBytesOnTiers()).build();
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
        .setContainerHost(workerNetAddress.getContainerHost())
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
   * @param commandType wire type
   * @return proto representation of given wire type
   */
  public static alluxio.grpc.CommandType toProto(CommandType commandType) {
    return alluxio.grpc.CommandType.valueOf(commandType.name());
  }

  /**
   * @param persistFile wire type
   * @return proto representation of given wire type
   */
  public static alluxio.grpc.PersistFile toProto(PersistFile persistFile) {
    return alluxio.grpc.PersistFile.newBuilder().setFileId(persistFile.getFileId())
        .addAllBlockIds(persistFile.getBlockIds()).build();
  }

  /**
   * @param fsCommand wire type
   * @return proto representation of given wire type
   */
  public static alluxio.grpc.FileSystemCommand toProto(FileSystemCommand fsCommand) {

    return alluxio.grpc.FileSystemCommand.newBuilder()
        .setCommandType(toProto(fsCommand.getCommandType()))
        .setCommandOptions(FileSystemCommandOptions.newBuilder()
            .setPersistOptions(PersistCommandOptions.newBuilder().addAllPersistFiles(
                fsCommand.getCommandOptions().getPersistOptions().getFilesToPersist().stream()
                    .map(GrpcUtils::toProto).collect(Collectors.toList()))))
        .build();
  }

  /**
   * @param ufsInfo wire type
   * @return proto representation of given wire type
   */
  public static alluxio.grpc.UfsInfo toProto(UfsInfo ufsInfo) {
    return alluxio.grpc.UfsInfo.newBuilder().setUri(ufsInfo.getUri().toString())
        .setProperties(ufsInfo.getMountOptions()).build();
  }

  /**
   * @param source source enum
   * @param target target enum
   * @return true if target enum is contained within the source
   */
  public static boolean contains(Scope source, Scope target) {
    return (source.getNumber() | target.getNumber()) == source.getNumber();
  }

  /**
   * @param scope1 source1
   * @param scope2 source2
   * @return combined enum of given enums
   */
  public static Scope combine(Scope scope1, Scope scope2) {
    return Scope.forNumber(scope1.getNumber() & scope2.getNumber());
  }
}
