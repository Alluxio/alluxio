// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/version.proto

package alluxio.grpc;

public final class VersionProto {
  private VersionProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_version_GetServiceVersionPRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_version_GetServiceVersionPRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_version_GetServiceVersionPResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_version_GetServiceVersionPResponse_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\022grpc/version.proto\022\024alluxio.grpc.versi" +
      "on\"S\n\031GetServiceVersionPRequest\0226\n\013servi" +
      "ceType\030\001 \001(\0162!.alluxio.grpc.version.Serv" +
      "iceType\"-\n\032GetServiceVersionPResponse\022\017\n" +
      "\007version\030\001 \001(\003*\344\003\n\013ServiceType\022\023\n\017UNKNOW" +
      "N_SERVICE\020\000\022%\n!FILE_SYSTEM_MASTER_CLIENT" +
      "_SERVICE\020\001\022%\n!FILE_SYSTEM_MASTER_WORKER_" +
      "SERVICE\020\002\022\"\n\036FILE_SYSTEM_MASTER_JOB_SERV" +
      "ICE\020\003\022\037\n\033BLOCK_MASTER_CLIENT_SERVICE\020\004\022\037" +
      "\n\033BLOCK_MASTER_WORKER_SERVICE\020\005\022\036\n\032META_" +
      "MASTER_CONFIG_SERVICE\020\006\022\036\n\032META_MASTER_C" +
      "LIENT_SERVICE\020\007\022\036\n\032META_MASTER_MASTER_SE" +
      "RVICE\020\010\022!\n\035METRICS_MASTER_CLIENT_SERVICE" +
      "\020\t\022\035\n\031JOB_MASTER_CLIENT_SERVICE\020\n\022\035\n\031JOB" +
      "_MASTER_WORKER_SERVICE\020\013\022%\n!FILE_SYSTEM_" +
      "WORKER_WORKER_SERVICE\020\014\022$\n\037PRIVILEGE_MAS" +
      "TER_CLIENT_SERVICE\020\351\0072\225\001\n\033ServiceVersion" +
      "ClientService\022v\n\021getServiceVersion\022/.all" +
      "uxio.grpc.version.GetServiceVersionPRequ" +
      "est\0320.alluxio.grpc.version.GetServiceVer" +
      "sionPResponseB\036\n\014alluxio.grpcB\014VersionPr" +
      "otoP\001"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
    internal_static_alluxio_grpc_version_GetServiceVersionPRequest_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_alluxio_grpc_version_GetServiceVersionPRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_version_GetServiceVersionPRequest_descriptor,
        new java.lang.String[] { "ServiceType", });
    internal_static_alluxio_grpc_version_GetServiceVersionPResponse_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_alluxio_grpc_version_GetServiceVersionPResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_version_GetServiceVersionPResponse_descriptor,
        new java.lang.String[] { "Version", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
