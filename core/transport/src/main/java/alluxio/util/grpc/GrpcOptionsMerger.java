package alluxio.util.grpc;

import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.file.FileSystemMasterOptions;

/**
 * Provides methods to merge client options with master options.
 */
public final class GrpcOptionsMerger {
    private GrpcOptionsMerger() {} // prevent instantiation

    /**
     * Converts from proto type to options.
     *
     * @param masterOptions the default master options provider
     * @param pOptions the proto options to convert
     * @return the converted options instance
     */
    public static FileSystemMasterCommonPOptions fromProtoToProto(
            FileSystemMasterOptions masterOptions, FileSystemMasterCommonPOptions pOptions) {
        FileSystemMasterCommonPOptions.Builder optionsBuilder =
                GrpcUtils.toProto(masterOptions.getCommonOptions()).toBuilder();
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
     * @param pOptions the proto options to convert
     * @return the converted options instance
     */
    public static FreePOptions merge(FileSystemMasterOptions masterOptions,
                                     FreePOptions pOptions) {
        FreePOptions.Builder optionsBuilder = masterOptions.getFreeOptions().toBuilder();
        if (pOptions != null) {
            if (pOptions.hasCommonOptions()) {
                optionsBuilder
                        .setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
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
     * @param pOptions the proto options to convert
     * @return the converted options instance
     */
    public static GetStatusPOptions merge(FileSystemMasterOptions masterOptions,
                                          GetStatusPOptions pOptions) {
        GetStatusPOptions.Builder optionsBuilder = masterOptions.getGetStatusOptions().toBuilder();
        if (pOptions != null) {
            if (pOptions.hasCommonOptions()) {
                optionsBuilder
                        .setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
            }
            if (pOptions.hasLoadMetadataType()) {
                optionsBuilder.setLoadMetadataType(pOptions.getLoadMetadataType());
            }
        }
        return optionsBuilder.build();
    }

    /**
     * Converts from proto type to options.
     *
     * @param masterOptions the default master options provider
     * @param pOptions the proto options to convert
     * @return the converted options instance
     */
    public static ListStatusPOptions merge(FileSystemMasterOptions masterOptions,
                                           ListStatusPOptions pOptions) {
        ListStatusPOptions.Builder optionsBuilder = masterOptions.getListStatusOptions().toBuilder();
        if (pOptions != null) {
            if (pOptions.hasCommonOptions()) {
                optionsBuilder
                        .setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
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
     * Converts from proto type to options.
     *
     * @param masterOptions the default master options provider
     * @param pOptions the proto options to convert
     * @return the converted options instance
     */
    public static MountPOptions merge(FileSystemMasterOptions masterOptions,
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
     * @param pOptions the proto options to convert
     * @return the converted options instance
     */
    public static DeletePOptions merge(FileSystemMasterOptions masterOptions,
                                       DeletePOptions pOptions) {
        DeletePOptions.Builder optionsBuilder = masterOptions.getDeleteOptions().toBuilder();
        if (pOptions != null) {
            if (pOptions.hasCommonOptions()) {
                optionsBuilder
                        .setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
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
     * @param pOptions the proto options to convert
     * @return the converted options instance
     */
    public static RenamePOptions merge(FileSystemMasterOptions masterOptions,
                                       RenamePOptions pOptions) {
        RenamePOptions.Builder optionsBuilder = masterOptions.getRenameOptions().toBuilder();
        if (pOptions != null) {
            if (pOptions.hasCommonOptions()) {
                optionsBuilder
                        .setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
            }
        }
        return optionsBuilder.build();
    }

    /**
     * Converts from proto type to options.
     *
     * @param masterOptions the default master options provider
     * @param pOptions the proto options to convert
     * @return the converted options instance
     */
    public static SetAclPOptions merge(FileSystemMasterOptions masterOptions,
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
     * @param pOptions the proto options to convert
     * @return the converted options instance
     */
    public static CheckConsistencyPOptions merge(FileSystemMasterOptions masterOptions,
                                                 CheckConsistencyPOptions pOptions) {
        CheckConsistencyPOptions.Builder optionsBuilder =
                masterOptions.getCheckConsistencyOptions().toBuilder();
        if (pOptions != null) {
            if (pOptions.hasCommonOptions()) {
                optionsBuilder
                        .setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
            }
        }
        return optionsBuilder.build();
    }

    /**
     * Converts from proto type to options.
     *
     * @param masterOptions the default master options provider
     * @param pOptions the proto options to convert
     * @return the converted options instance
     */
    public static SetAttributePOptions merge(FileSystemMasterOptions masterOptions,
                                             SetAttributePOptions pOptions) {
        SetAttributePOptions.Builder optionsBuilder =
                masterOptions.getSetAttributeOptions().toBuilder();
        if (pOptions != null) {
            if (pOptions.hasCommonOptions()) {
                optionsBuilder
                        .setCommonOptions(fromProtoToProto(masterOptions, pOptions.getCommonOptions()));
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
}
