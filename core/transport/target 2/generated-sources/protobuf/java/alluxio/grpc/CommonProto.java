// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/common.proto

package alluxio.grpc;

public final class CommonProto {
  private CommonProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_PMode_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_PMode_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_BlockInfo_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_BlockInfo_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_BlockLocation_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_BlockLocation_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_Metric_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_Metric_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_Metric_TagsEntry_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_Metric_TagsEntry_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_ConfigProperty_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_ConfigProperty_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_Command_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_Command_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_LocalityTier_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_LocalityTier_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_TieredIdentity_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_TieredIdentity_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_NetAddress_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_NetAddress_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_WorkerNetAddress_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_WorkerNetAddress_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_BlockStoreLocationProto_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_BlockStoreLocationProto_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_RetryInfo_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_RetryInfo_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_ErrorInfo_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_ErrorInfo_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\021grpc/common.proto\022\014alluxio.grpc\"|\n\005PMo" +
      "de\022%\n\townerBits\030\001 \002(\0162\022.alluxio.grpc.Bit" +
      "s\022%\n\tgroupBits\030\002 \002(\0162\022.alluxio.grpc.Bits" +
      "\022%\n\totherBits\030\003 \002(\0162\022.alluxio.grpc.Bits\"" +
      "\\\n\tBlockInfo\022\017\n\007blockId\030\001 \001(\003\022\016\n\006length\030" +
      "\002 \001(\003\022.\n\tlocations\030\003 \003(\0132\033.alluxio.grpc." +
      "BlockLocation\"\177\n\rBlockLocation\022\020\n\010worker" +
      "Id\030\001 \001(\003\0225\n\rworkerAddress\030\002 \001(\0132\036.alluxi" +
      "o.grpc.WorkerNetAddress\022\021\n\ttierAlias\030\003 \001" +
      "(\t\022\022\n\nmediumType\030\004 \001(\t\"\320\001\n\006Metric\022\020\n\010ins" +
      "tance\030\001 \001(\t\022\016\n\006source\030\002 \001(\t\022\014\n\004name\030\003 \001(" +
      "\t\022\r\n\005value\030\004 \001(\001\022,\n\nmetricType\030\005 \002(\0162\030.a" +
      "lluxio.grpc.MetricType\022,\n\004tags\030\006 \003(\0132\036.a" +
      "lluxio.grpc.Metric.TagsEntry\032+\n\tTagsEntr" +
      "y\022\013\n\003key\030\001 \001(\t\022\r\n\005value\030\002 \001(\t:\0028\001\"=\n\016Con" +
      "figProperty\022\014\n\004name\030\001 \001(\t\022\016\n\006source\030\002 \001(" +
      "\t\022\r\n\005value\030\003 \001(\t\"G\n\007Command\022.\n\013commandTy" +
      "pe\030\001 \001(\0162\031.alluxio.grpc.CommandType\022\014\n\004d" +
      "ata\030\002 \003(\003\"/\n\014LocalityTier\022\020\n\010tierName\030\001 " +
      "\001(\t\022\r\n\005value\030\002 \001(\t\";\n\016TieredIdentity\022)\n\005" +
      "tiers\030\001 \003(\0132\032.alluxio.grpc.LocalityTier\"" +
      "+\n\nNetAddress\022\014\n\004host\030\001 \001(\t\022\017\n\007rpcPort\030\002" +
      " \001(\005\"\273\001\n\020WorkerNetAddress\022\014\n\004host\030\001 \001(\t\022" +
      "\017\n\007rpcPort\030\002 \001(\005\022\020\n\010dataPort\030\003 \001(\005\022\017\n\007we" +
      "bPort\030\004 \001(\005\022\030\n\020domainSocketPath\030\005 \001(\t\0224\n" +
      "\016tieredIdentity\030\006 \001(\0132\034.alluxio.grpc.Tie" +
      "redIdentity\022\025\n\rcontainerHost\030\007 \001(\t\"@\n\027Bl" +
      "ockStoreLocationProto\022\021\n\ttierAlias\030\001 \001(\t" +
      "\022\022\n\nmediumType\030\002 \001(\t\" \n\tRetryInfo\022\023\n\013isR" +
      "etryable\030\001 \001(\010\"7\n\tErrorInfo\022*\n\terrorType" +
      "\030\001 \001(\0162\027.alluxio.grpc.ErrorType*p\n\004Bits\022" +
      "\010\n\004NONE\020\001\022\013\n\007EXECUTE\020\002\022\t\n\005WRITE\020\003\022\021\n\rWRI" +
      "TE_EXECUTE\020\004\022\010\n\004READ\020\005\022\020\n\014READ_EXECUTE\020\006" +
      "\022\016\n\nREAD_WRITE\020\007\022\007\n\003ALL\020\010*P\n\nMetricType\022" +
      "\t\n\005GAUGE\020\000\022\013\n\007COUNTER\020\001\022\t\n\005METER\020\002\022\t\n\005TI" +
      "MER\020\003\022\024\n\020EXECUTOR_SERVICE\020\004*h\n\013CommandTy" +
      "pe\022\013\n\007Unknown\020\000\022\013\n\007Nothing\020\001\022\014\n\010Register" +
      "\020\002\022\010\n\004Free\020\003\022\n\n\006Delete\020\004\022\013\n\007Persist\020\005\022\016\n" +
      "\nFreeWorker\020\006*!\n\tTtlAction\022\n\n\006DELETE\020\000\022\010" +
      "\n\004FREE\020\001*1\n\tErrorType\022\010\n\004User\020\000\022\014\n\010Inter" +
      "nal\020\001\022\014\n\010External\020\002B\035\n\014alluxio.grpcB\013Com" +
      "monProtoP\001"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        });
    internal_static_alluxio_grpc_PMode_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_alluxio_grpc_PMode_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_PMode_descriptor,
        new java.lang.String[] { "OwnerBits", "GroupBits", "OtherBits", });
    internal_static_alluxio_grpc_BlockInfo_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_alluxio_grpc_BlockInfo_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_BlockInfo_descriptor,
        new java.lang.String[] { "BlockId", "Length", "Locations", });
    internal_static_alluxio_grpc_BlockLocation_descriptor =
      getDescriptor().getMessageTypes().get(2);
    internal_static_alluxio_grpc_BlockLocation_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_BlockLocation_descriptor,
        new java.lang.String[] { "WorkerId", "WorkerAddress", "TierAlias", "MediumType", });
    internal_static_alluxio_grpc_Metric_descriptor =
      getDescriptor().getMessageTypes().get(3);
    internal_static_alluxio_grpc_Metric_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_Metric_descriptor,
        new java.lang.String[] { "Instance", "Source", "Name", "Value", "MetricType", "Tags", });
    internal_static_alluxio_grpc_Metric_TagsEntry_descriptor =
      internal_static_alluxio_grpc_Metric_descriptor.getNestedTypes().get(0);
    internal_static_alluxio_grpc_Metric_TagsEntry_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_Metric_TagsEntry_descriptor,
        new java.lang.String[] { "Key", "Value", });
    internal_static_alluxio_grpc_ConfigProperty_descriptor =
      getDescriptor().getMessageTypes().get(4);
    internal_static_alluxio_grpc_ConfigProperty_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_ConfigProperty_descriptor,
        new java.lang.String[] { "Name", "Source", "Value", });
    internal_static_alluxio_grpc_Command_descriptor =
      getDescriptor().getMessageTypes().get(5);
    internal_static_alluxio_grpc_Command_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_Command_descriptor,
        new java.lang.String[] { "CommandType", "Data", });
    internal_static_alluxio_grpc_LocalityTier_descriptor =
      getDescriptor().getMessageTypes().get(6);
    internal_static_alluxio_grpc_LocalityTier_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_LocalityTier_descriptor,
        new java.lang.String[] { "TierName", "Value", });
    internal_static_alluxio_grpc_TieredIdentity_descriptor =
      getDescriptor().getMessageTypes().get(7);
    internal_static_alluxio_grpc_TieredIdentity_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_TieredIdentity_descriptor,
        new java.lang.String[] { "Tiers", });
    internal_static_alluxio_grpc_NetAddress_descriptor =
      getDescriptor().getMessageTypes().get(8);
    internal_static_alluxio_grpc_NetAddress_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_NetAddress_descriptor,
        new java.lang.String[] { "Host", "RpcPort", });
    internal_static_alluxio_grpc_WorkerNetAddress_descriptor =
      getDescriptor().getMessageTypes().get(9);
    internal_static_alluxio_grpc_WorkerNetAddress_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_WorkerNetAddress_descriptor,
        new java.lang.String[] { "Host", "RpcPort", "DataPort", "WebPort", "DomainSocketPath", "TieredIdentity", "ContainerHost", });
    internal_static_alluxio_grpc_BlockStoreLocationProto_descriptor =
      getDescriptor().getMessageTypes().get(10);
    internal_static_alluxio_grpc_BlockStoreLocationProto_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_BlockStoreLocationProto_descriptor,
        new java.lang.String[] { "TierAlias", "MediumType", });
    internal_static_alluxio_grpc_RetryInfo_descriptor =
      getDescriptor().getMessageTypes().get(11);
    internal_static_alluxio_grpc_RetryInfo_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_RetryInfo_descriptor,
        new java.lang.String[] { "IsRetryable", });
    internal_static_alluxio_grpc_ErrorInfo_descriptor =
      getDescriptor().getMessageTypes().get(12);
    internal_static_alluxio_grpc_ErrorInfo_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_ErrorInfo_descriptor,
        new java.lang.String[] { "ErrorType", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
