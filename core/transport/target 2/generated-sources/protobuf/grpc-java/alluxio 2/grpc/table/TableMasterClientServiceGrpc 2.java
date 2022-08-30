package alluxio.grpc.table;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 **
 * This interface contains table master service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.37.0)",
    comments = "Source: grpc/table/table_master.proto")
public final class TableMasterClientServiceGrpc {

  private TableMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.table.TableMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.table.GetAllDatabasesPRequest,
      alluxio.grpc.table.GetAllDatabasesPResponse> getGetAllDatabasesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAllDatabases",
      requestType = alluxio.grpc.table.GetAllDatabasesPRequest.class,
      responseType = alluxio.grpc.table.GetAllDatabasesPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.table.GetAllDatabasesPRequest,
      alluxio.grpc.table.GetAllDatabasesPResponse> getGetAllDatabasesMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.table.GetAllDatabasesPRequest, alluxio.grpc.table.GetAllDatabasesPResponse> getGetAllDatabasesMethod;
    if ((getGetAllDatabasesMethod = TableMasterClientServiceGrpc.getGetAllDatabasesMethod) == null) {
      synchronized (TableMasterClientServiceGrpc.class) {
        if ((getGetAllDatabasesMethod = TableMasterClientServiceGrpc.getGetAllDatabasesMethod) == null) {
          TableMasterClientServiceGrpc.getGetAllDatabasesMethod = getGetAllDatabasesMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.table.GetAllDatabasesPRequest, alluxio.grpc.table.GetAllDatabasesPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetAllDatabases"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.GetAllDatabasesPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.GetAllDatabasesPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TableMasterClientServiceMethodDescriptorSupplier("GetAllDatabases"))
              .build();
        }
      }
    }
    return getGetAllDatabasesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.table.GetAllTablesPRequest,
      alluxio.grpc.table.GetAllTablesPResponse> getGetAllTablesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAllTables",
      requestType = alluxio.grpc.table.GetAllTablesPRequest.class,
      responseType = alluxio.grpc.table.GetAllTablesPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.table.GetAllTablesPRequest,
      alluxio.grpc.table.GetAllTablesPResponse> getGetAllTablesMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.table.GetAllTablesPRequest, alluxio.grpc.table.GetAllTablesPResponse> getGetAllTablesMethod;
    if ((getGetAllTablesMethod = TableMasterClientServiceGrpc.getGetAllTablesMethod) == null) {
      synchronized (TableMasterClientServiceGrpc.class) {
        if ((getGetAllTablesMethod = TableMasterClientServiceGrpc.getGetAllTablesMethod) == null) {
          TableMasterClientServiceGrpc.getGetAllTablesMethod = getGetAllTablesMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.table.GetAllTablesPRequest, alluxio.grpc.table.GetAllTablesPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetAllTables"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.GetAllTablesPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.GetAllTablesPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TableMasterClientServiceMethodDescriptorSupplier("GetAllTables"))
              .build();
        }
      }
    }
    return getGetAllTablesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.table.GetDatabasePRequest,
      alluxio.grpc.table.GetDatabasePResponse> getGetDatabaseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetDatabase",
      requestType = alluxio.grpc.table.GetDatabasePRequest.class,
      responseType = alluxio.grpc.table.GetDatabasePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.table.GetDatabasePRequest,
      alluxio.grpc.table.GetDatabasePResponse> getGetDatabaseMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.table.GetDatabasePRequest, alluxio.grpc.table.GetDatabasePResponse> getGetDatabaseMethod;
    if ((getGetDatabaseMethod = TableMasterClientServiceGrpc.getGetDatabaseMethod) == null) {
      synchronized (TableMasterClientServiceGrpc.class) {
        if ((getGetDatabaseMethod = TableMasterClientServiceGrpc.getGetDatabaseMethod) == null) {
          TableMasterClientServiceGrpc.getGetDatabaseMethod = getGetDatabaseMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.table.GetDatabasePRequest, alluxio.grpc.table.GetDatabasePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetDatabase"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.GetDatabasePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.GetDatabasePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TableMasterClientServiceMethodDescriptorSupplier("GetDatabase"))
              .build();
        }
      }
    }
    return getGetDatabaseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.table.GetTablePRequest,
      alluxio.grpc.table.GetTablePResponse> getGetTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTable",
      requestType = alluxio.grpc.table.GetTablePRequest.class,
      responseType = alluxio.grpc.table.GetTablePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.table.GetTablePRequest,
      alluxio.grpc.table.GetTablePResponse> getGetTableMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.table.GetTablePRequest, alluxio.grpc.table.GetTablePResponse> getGetTableMethod;
    if ((getGetTableMethod = TableMasterClientServiceGrpc.getGetTableMethod) == null) {
      synchronized (TableMasterClientServiceGrpc.class) {
        if ((getGetTableMethod = TableMasterClientServiceGrpc.getGetTableMethod) == null) {
          TableMasterClientServiceGrpc.getGetTableMethod = getGetTableMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.table.GetTablePRequest, alluxio.grpc.table.GetTablePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.GetTablePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.GetTablePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TableMasterClientServiceMethodDescriptorSupplier("GetTable"))
              .build();
        }
      }
    }
    return getGetTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.table.AttachDatabasePRequest,
      alluxio.grpc.table.AttachDatabasePResponse> getAttachDatabaseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AttachDatabase",
      requestType = alluxio.grpc.table.AttachDatabasePRequest.class,
      responseType = alluxio.grpc.table.AttachDatabasePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.table.AttachDatabasePRequest,
      alluxio.grpc.table.AttachDatabasePResponse> getAttachDatabaseMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.table.AttachDatabasePRequest, alluxio.grpc.table.AttachDatabasePResponse> getAttachDatabaseMethod;
    if ((getAttachDatabaseMethod = TableMasterClientServiceGrpc.getAttachDatabaseMethod) == null) {
      synchronized (TableMasterClientServiceGrpc.class) {
        if ((getAttachDatabaseMethod = TableMasterClientServiceGrpc.getAttachDatabaseMethod) == null) {
          TableMasterClientServiceGrpc.getAttachDatabaseMethod = getAttachDatabaseMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.table.AttachDatabasePRequest, alluxio.grpc.table.AttachDatabasePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AttachDatabase"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.AttachDatabasePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.AttachDatabasePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TableMasterClientServiceMethodDescriptorSupplier("AttachDatabase"))
              .build();
        }
      }
    }
    return getAttachDatabaseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.table.DetachDatabasePRequest,
      alluxio.grpc.table.DetachDatabasePResponse> getDetachDatabaseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DetachDatabase",
      requestType = alluxio.grpc.table.DetachDatabasePRequest.class,
      responseType = alluxio.grpc.table.DetachDatabasePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.table.DetachDatabasePRequest,
      alluxio.grpc.table.DetachDatabasePResponse> getDetachDatabaseMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.table.DetachDatabasePRequest, alluxio.grpc.table.DetachDatabasePResponse> getDetachDatabaseMethod;
    if ((getDetachDatabaseMethod = TableMasterClientServiceGrpc.getDetachDatabaseMethod) == null) {
      synchronized (TableMasterClientServiceGrpc.class) {
        if ((getDetachDatabaseMethod = TableMasterClientServiceGrpc.getDetachDatabaseMethod) == null) {
          TableMasterClientServiceGrpc.getDetachDatabaseMethod = getDetachDatabaseMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.table.DetachDatabasePRequest, alluxio.grpc.table.DetachDatabasePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DetachDatabase"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.DetachDatabasePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.DetachDatabasePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TableMasterClientServiceMethodDescriptorSupplier("DetachDatabase"))
              .build();
        }
      }
    }
    return getDetachDatabaseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.table.SyncDatabasePRequest,
      alluxio.grpc.table.SyncDatabasePResponse> getSyncDatabaseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SyncDatabase",
      requestType = alluxio.grpc.table.SyncDatabasePRequest.class,
      responseType = alluxio.grpc.table.SyncDatabasePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.table.SyncDatabasePRequest,
      alluxio.grpc.table.SyncDatabasePResponse> getSyncDatabaseMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.table.SyncDatabasePRequest, alluxio.grpc.table.SyncDatabasePResponse> getSyncDatabaseMethod;
    if ((getSyncDatabaseMethod = TableMasterClientServiceGrpc.getSyncDatabaseMethod) == null) {
      synchronized (TableMasterClientServiceGrpc.class) {
        if ((getSyncDatabaseMethod = TableMasterClientServiceGrpc.getSyncDatabaseMethod) == null) {
          TableMasterClientServiceGrpc.getSyncDatabaseMethod = getSyncDatabaseMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.table.SyncDatabasePRequest, alluxio.grpc.table.SyncDatabasePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SyncDatabase"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.SyncDatabasePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.SyncDatabasePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TableMasterClientServiceMethodDescriptorSupplier("SyncDatabase"))
              .build();
        }
      }
    }
    return getSyncDatabaseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.table.GetTableColumnStatisticsPRequest,
      alluxio.grpc.table.GetTableColumnStatisticsPResponse> getGetTableColumnStatisticsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTableColumnStatistics",
      requestType = alluxio.grpc.table.GetTableColumnStatisticsPRequest.class,
      responseType = alluxio.grpc.table.GetTableColumnStatisticsPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.table.GetTableColumnStatisticsPRequest,
      alluxio.grpc.table.GetTableColumnStatisticsPResponse> getGetTableColumnStatisticsMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.table.GetTableColumnStatisticsPRequest, alluxio.grpc.table.GetTableColumnStatisticsPResponse> getGetTableColumnStatisticsMethod;
    if ((getGetTableColumnStatisticsMethod = TableMasterClientServiceGrpc.getGetTableColumnStatisticsMethod) == null) {
      synchronized (TableMasterClientServiceGrpc.class) {
        if ((getGetTableColumnStatisticsMethod = TableMasterClientServiceGrpc.getGetTableColumnStatisticsMethod) == null) {
          TableMasterClientServiceGrpc.getGetTableColumnStatisticsMethod = getGetTableColumnStatisticsMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.table.GetTableColumnStatisticsPRequest, alluxio.grpc.table.GetTableColumnStatisticsPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTableColumnStatistics"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.GetTableColumnStatisticsPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.GetTableColumnStatisticsPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TableMasterClientServiceMethodDescriptorSupplier("GetTableColumnStatistics"))
              .build();
        }
      }
    }
    return getGetTableColumnStatisticsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.table.GetPartitionColumnStatisticsPRequest,
      alluxio.grpc.table.GetPartitionColumnStatisticsPResponse> getGetPartitionColumnStatisticsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPartitionColumnStatistics",
      requestType = alluxio.grpc.table.GetPartitionColumnStatisticsPRequest.class,
      responseType = alluxio.grpc.table.GetPartitionColumnStatisticsPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.table.GetPartitionColumnStatisticsPRequest,
      alluxio.grpc.table.GetPartitionColumnStatisticsPResponse> getGetPartitionColumnStatisticsMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.table.GetPartitionColumnStatisticsPRequest, alluxio.grpc.table.GetPartitionColumnStatisticsPResponse> getGetPartitionColumnStatisticsMethod;
    if ((getGetPartitionColumnStatisticsMethod = TableMasterClientServiceGrpc.getGetPartitionColumnStatisticsMethod) == null) {
      synchronized (TableMasterClientServiceGrpc.class) {
        if ((getGetPartitionColumnStatisticsMethod = TableMasterClientServiceGrpc.getGetPartitionColumnStatisticsMethod) == null) {
          TableMasterClientServiceGrpc.getGetPartitionColumnStatisticsMethod = getGetPartitionColumnStatisticsMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.table.GetPartitionColumnStatisticsPRequest, alluxio.grpc.table.GetPartitionColumnStatisticsPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetPartitionColumnStatistics"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.GetPartitionColumnStatisticsPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.GetPartitionColumnStatisticsPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TableMasterClientServiceMethodDescriptorSupplier("GetPartitionColumnStatistics"))
              .build();
        }
      }
    }
    return getGetPartitionColumnStatisticsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.table.ReadTablePRequest,
      alluxio.grpc.table.ReadTablePResponse> getReadTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReadTable",
      requestType = alluxio.grpc.table.ReadTablePRequest.class,
      responseType = alluxio.grpc.table.ReadTablePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.table.ReadTablePRequest,
      alluxio.grpc.table.ReadTablePResponse> getReadTableMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.table.ReadTablePRequest, alluxio.grpc.table.ReadTablePResponse> getReadTableMethod;
    if ((getReadTableMethod = TableMasterClientServiceGrpc.getReadTableMethod) == null) {
      synchronized (TableMasterClientServiceGrpc.class) {
        if ((getReadTableMethod = TableMasterClientServiceGrpc.getReadTableMethod) == null) {
          TableMasterClientServiceGrpc.getReadTableMethod = getReadTableMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.table.ReadTablePRequest, alluxio.grpc.table.ReadTablePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReadTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.ReadTablePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.ReadTablePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TableMasterClientServiceMethodDescriptorSupplier("ReadTable"))
              .build();
        }
      }
    }
    return getReadTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.table.TransformTablePRequest,
      alluxio.grpc.table.TransformTablePResponse> getTransformTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TransformTable",
      requestType = alluxio.grpc.table.TransformTablePRequest.class,
      responseType = alluxio.grpc.table.TransformTablePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.table.TransformTablePRequest,
      alluxio.grpc.table.TransformTablePResponse> getTransformTableMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.table.TransformTablePRequest, alluxio.grpc.table.TransformTablePResponse> getTransformTableMethod;
    if ((getTransformTableMethod = TableMasterClientServiceGrpc.getTransformTableMethod) == null) {
      synchronized (TableMasterClientServiceGrpc.class) {
        if ((getTransformTableMethod = TableMasterClientServiceGrpc.getTransformTableMethod) == null) {
          TableMasterClientServiceGrpc.getTransformTableMethod = getTransformTableMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.table.TransformTablePRequest, alluxio.grpc.table.TransformTablePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TransformTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.TransformTablePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.TransformTablePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TableMasterClientServiceMethodDescriptorSupplier("TransformTable"))
              .build();
        }
      }
    }
    return getTransformTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.table.GetTransformJobInfoPRequest,
      alluxio.grpc.table.GetTransformJobInfoPResponse> getGetTransformJobInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTransformJobInfo",
      requestType = alluxio.grpc.table.GetTransformJobInfoPRequest.class,
      responseType = alluxio.grpc.table.GetTransformJobInfoPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.table.GetTransformJobInfoPRequest,
      alluxio.grpc.table.GetTransformJobInfoPResponse> getGetTransformJobInfoMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.table.GetTransformJobInfoPRequest, alluxio.grpc.table.GetTransformJobInfoPResponse> getGetTransformJobInfoMethod;
    if ((getGetTransformJobInfoMethod = TableMasterClientServiceGrpc.getGetTransformJobInfoMethod) == null) {
      synchronized (TableMasterClientServiceGrpc.class) {
        if ((getGetTransformJobInfoMethod = TableMasterClientServiceGrpc.getGetTransformJobInfoMethod) == null) {
          TableMasterClientServiceGrpc.getGetTransformJobInfoMethod = getGetTransformJobInfoMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.table.GetTransformJobInfoPRequest, alluxio.grpc.table.GetTransformJobInfoPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTransformJobInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.GetTransformJobInfoPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.table.GetTransformJobInfoPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TableMasterClientServiceMethodDescriptorSupplier("GetTransformJobInfo"))
              .build();
        }
      }
    }
    return getGetTransformJobInfoMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TableMasterClientServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TableMasterClientServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TableMasterClientServiceStub>() {
        @java.lang.Override
        public TableMasterClientServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TableMasterClientServiceStub(channel, callOptions);
        }
      };
    return TableMasterClientServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TableMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TableMasterClientServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TableMasterClientServiceBlockingStub>() {
        @java.lang.Override
        public TableMasterClientServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TableMasterClientServiceBlockingStub(channel, callOptions);
        }
      };
    return TableMasterClientServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TableMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TableMasterClientServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TableMasterClientServiceFutureStub>() {
        @java.lang.Override
        public TableMasterClientServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TableMasterClientServiceFutureStub(channel, callOptions);
        }
      };
    return TableMasterClientServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   **
   * This interface contains table master service endpoints for Alluxio clients.
   * </pre>
   */
  public static abstract class TableMasterClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Returns all databases in the catalog
     * </pre>
     */
    public void getAllDatabases(alluxio.grpc.table.GetAllDatabasesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.GetAllDatabasesPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetAllDatabasesMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns all tables in the database
     * </pre>
     */
    public void getAllTables(alluxio.grpc.table.GetAllTablesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.GetAllTablesPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetAllTablesMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Gets a database by name from the table master
     * </pre>
     */
    public void getDatabase(alluxio.grpc.table.GetDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.GetDatabasePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetDatabaseMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a specific table info
     * </pre>
     */
    public void getTable(alluxio.grpc.table.GetTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.GetTablePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTableMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Attach an existing database into the catalog as a new database name
     * </pre>
     */
    public void attachDatabase(alluxio.grpc.table.AttachDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.AttachDatabasePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAttachDatabaseMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Detach existing database into the catalog, removing any metadata about the table
     * </pre>
     */
    public void detachDatabase(alluxio.grpc.table.DetachDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.DetachDatabasePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDetachDatabaseMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Sync existing database into the catalog
     * </pre>
     */
    public void syncDatabase(alluxio.grpc.table.SyncDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.SyncDatabasePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSyncDatabaseMethod(), responseObserver);
    }

    /**
     */
    public void getTableColumnStatistics(alluxio.grpc.table.GetTableColumnStatisticsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.GetTableColumnStatisticsPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTableColumnStatisticsMethod(), responseObserver);
    }

    /**
     */
    public void getPartitionColumnStatistics(alluxio.grpc.table.GetPartitionColumnStatisticsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.GetPartitionColumnStatisticsPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetPartitionColumnStatisticsMethod(), responseObserver);
    }

    /**
     */
    public void readTable(alluxio.grpc.table.ReadTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.ReadTablePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReadTableMethod(), responseObserver);
    }

    /**
     */
    public void transformTable(alluxio.grpc.table.TransformTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.TransformTablePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTransformTableMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Gets information of transformation jobs.
     * If the job ID exists in the request, the information for that job is returned;
     * Otherwise, information of all the jobs kept in table master will be returned.
     * </pre>
     */
    public void getTransformJobInfo(alluxio.grpc.table.GetTransformJobInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.GetTransformJobInfoPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTransformJobInfoMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetAllDatabasesMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.table.GetAllDatabasesPRequest,
                alluxio.grpc.table.GetAllDatabasesPResponse>(
                  this, METHODID_GET_ALL_DATABASES)))
          .addMethod(
            getGetAllTablesMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.table.GetAllTablesPRequest,
                alluxio.grpc.table.GetAllTablesPResponse>(
                  this, METHODID_GET_ALL_TABLES)))
          .addMethod(
            getGetDatabaseMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.table.GetDatabasePRequest,
                alluxio.grpc.table.GetDatabasePResponse>(
                  this, METHODID_GET_DATABASE)))
          .addMethod(
            getGetTableMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.table.GetTablePRequest,
                alluxio.grpc.table.GetTablePResponse>(
                  this, METHODID_GET_TABLE)))
          .addMethod(
            getAttachDatabaseMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.table.AttachDatabasePRequest,
                alluxio.grpc.table.AttachDatabasePResponse>(
                  this, METHODID_ATTACH_DATABASE)))
          .addMethod(
            getDetachDatabaseMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.table.DetachDatabasePRequest,
                alluxio.grpc.table.DetachDatabasePResponse>(
                  this, METHODID_DETACH_DATABASE)))
          .addMethod(
            getSyncDatabaseMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.table.SyncDatabasePRequest,
                alluxio.grpc.table.SyncDatabasePResponse>(
                  this, METHODID_SYNC_DATABASE)))
          .addMethod(
            getGetTableColumnStatisticsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.table.GetTableColumnStatisticsPRequest,
                alluxio.grpc.table.GetTableColumnStatisticsPResponse>(
                  this, METHODID_GET_TABLE_COLUMN_STATISTICS)))
          .addMethod(
            getGetPartitionColumnStatisticsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.table.GetPartitionColumnStatisticsPRequest,
                alluxio.grpc.table.GetPartitionColumnStatisticsPResponse>(
                  this, METHODID_GET_PARTITION_COLUMN_STATISTICS)))
          .addMethod(
            getReadTableMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.table.ReadTablePRequest,
                alluxio.grpc.table.ReadTablePResponse>(
                  this, METHODID_READ_TABLE)))
          .addMethod(
            getTransformTableMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.table.TransformTablePRequest,
                alluxio.grpc.table.TransformTablePResponse>(
                  this, METHODID_TRANSFORM_TABLE)))
          .addMethod(
            getGetTransformJobInfoMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.table.GetTransformJobInfoPRequest,
                alluxio.grpc.table.GetTransformJobInfoPResponse>(
                  this, METHODID_GET_TRANSFORM_JOB_INFO)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains table master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class TableMasterClientServiceStub extends io.grpc.stub.AbstractAsyncStub<TableMasterClientServiceStub> {
    private TableMasterClientServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TableMasterClientServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TableMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns all databases in the catalog
     * </pre>
     */
    public void getAllDatabases(alluxio.grpc.table.GetAllDatabasesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.GetAllDatabasesPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetAllDatabasesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns all tables in the database
     * </pre>
     */
    public void getAllTables(alluxio.grpc.table.GetAllTablesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.GetAllTablesPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetAllTablesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Gets a database by name from the table master
     * </pre>
     */
    public void getDatabase(alluxio.grpc.table.GetDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.GetDatabasePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetDatabaseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a specific table info
     * </pre>
     */
    public void getTable(alluxio.grpc.table.GetTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.GetTablePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Attach an existing database into the catalog as a new database name
     * </pre>
     */
    public void attachDatabase(alluxio.grpc.table.AttachDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.AttachDatabasePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAttachDatabaseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Detach existing database into the catalog, removing any metadata about the table
     * </pre>
     */
    public void detachDatabase(alluxio.grpc.table.DetachDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.DetachDatabasePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDetachDatabaseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Sync existing database into the catalog
     * </pre>
     */
    public void syncDatabase(alluxio.grpc.table.SyncDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.SyncDatabasePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSyncDatabaseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getTableColumnStatistics(alluxio.grpc.table.GetTableColumnStatisticsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.GetTableColumnStatisticsPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTableColumnStatisticsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getPartitionColumnStatistics(alluxio.grpc.table.GetPartitionColumnStatisticsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.GetPartitionColumnStatisticsPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetPartitionColumnStatisticsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void readTable(alluxio.grpc.table.ReadTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.ReadTablePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReadTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void transformTable(alluxio.grpc.table.TransformTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.TransformTablePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTransformTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Gets information of transformation jobs.
     * If the job ID exists in the request, the information for that job is returned;
     * Otherwise, information of all the jobs kept in table master will be returned.
     * </pre>
     */
    public void getTransformJobInfo(alluxio.grpc.table.GetTransformJobInfoPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.table.GetTransformJobInfoPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTransformJobInfoMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains table master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class TableMasterClientServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<TableMasterClientServiceBlockingStub> {
    private TableMasterClientServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TableMasterClientServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TableMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns all databases in the catalog
     * </pre>
     */
    public alluxio.grpc.table.GetAllDatabasesPResponse getAllDatabases(alluxio.grpc.table.GetAllDatabasesPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetAllDatabasesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns all tables in the database
     * </pre>
     */
    public alluxio.grpc.table.GetAllTablesPResponse getAllTables(alluxio.grpc.table.GetAllTablesPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetAllTablesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Gets a database by name from the table master
     * </pre>
     */
    public alluxio.grpc.table.GetDatabasePResponse getDatabase(alluxio.grpc.table.GetDatabasePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetDatabaseMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns a specific table info
     * </pre>
     */
    public alluxio.grpc.table.GetTablePResponse getTable(alluxio.grpc.table.GetTablePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTableMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Attach an existing database into the catalog as a new database name
     * </pre>
     */
    public alluxio.grpc.table.AttachDatabasePResponse attachDatabase(alluxio.grpc.table.AttachDatabasePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAttachDatabaseMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Detach existing database into the catalog, removing any metadata about the table
     * </pre>
     */
    public alluxio.grpc.table.DetachDatabasePResponse detachDatabase(alluxio.grpc.table.DetachDatabasePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDetachDatabaseMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Sync existing database into the catalog
     * </pre>
     */
    public alluxio.grpc.table.SyncDatabasePResponse syncDatabase(alluxio.grpc.table.SyncDatabasePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSyncDatabaseMethod(), getCallOptions(), request);
    }

    /**
     */
    public alluxio.grpc.table.GetTableColumnStatisticsPResponse getTableColumnStatistics(alluxio.grpc.table.GetTableColumnStatisticsPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTableColumnStatisticsMethod(), getCallOptions(), request);
    }

    /**
     */
    public alluxio.grpc.table.GetPartitionColumnStatisticsPResponse getPartitionColumnStatistics(alluxio.grpc.table.GetPartitionColumnStatisticsPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetPartitionColumnStatisticsMethod(), getCallOptions(), request);
    }

    /**
     */
    public alluxio.grpc.table.ReadTablePResponse readTable(alluxio.grpc.table.ReadTablePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReadTableMethod(), getCallOptions(), request);
    }

    /**
     */
    public alluxio.grpc.table.TransformTablePResponse transformTable(alluxio.grpc.table.TransformTablePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTransformTableMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Gets information of transformation jobs.
     * If the job ID exists in the request, the information for that job is returned;
     * Otherwise, information of all the jobs kept in table master will be returned.
     * </pre>
     */
    public alluxio.grpc.table.GetTransformJobInfoPResponse getTransformJobInfo(alluxio.grpc.table.GetTransformJobInfoPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTransformJobInfoMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains table master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class TableMasterClientServiceFutureStub extends io.grpc.stub.AbstractFutureStub<TableMasterClientServiceFutureStub> {
    private TableMasterClientServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TableMasterClientServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TableMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns all databases in the catalog
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.table.GetAllDatabasesPResponse> getAllDatabases(
        alluxio.grpc.table.GetAllDatabasesPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetAllDatabasesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns all tables in the database
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.table.GetAllTablesPResponse> getAllTables(
        alluxio.grpc.table.GetAllTablesPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetAllTablesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Gets a database by name from the table master
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.table.GetDatabasePResponse> getDatabase(
        alluxio.grpc.table.GetDatabasePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetDatabaseMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns a specific table info
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.table.GetTablePResponse> getTable(
        alluxio.grpc.table.GetTablePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTableMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Attach an existing database into the catalog as a new database name
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.table.AttachDatabasePResponse> attachDatabase(
        alluxio.grpc.table.AttachDatabasePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAttachDatabaseMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Detach existing database into the catalog, removing any metadata about the table
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.table.DetachDatabasePResponse> detachDatabase(
        alluxio.grpc.table.DetachDatabasePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDetachDatabaseMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Sync existing database into the catalog
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.table.SyncDatabasePResponse> syncDatabase(
        alluxio.grpc.table.SyncDatabasePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSyncDatabaseMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.table.GetTableColumnStatisticsPResponse> getTableColumnStatistics(
        alluxio.grpc.table.GetTableColumnStatisticsPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTableColumnStatisticsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.table.GetPartitionColumnStatisticsPResponse> getPartitionColumnStatistics(
        alluxio.grpc.table.GetPartitionColumnStatisticsPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetPartitionColumnStatisticsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.table.ReadTablePResponse> readTable(
        alluxio.grpc.table.ReadTablePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReadTableMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.table.TransformTablePResponse> transformTable(
        alluxio.grpc.table.TransformTablePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTransformTableMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Gets information of transformation jobs.
     * If the job ID exists in the request, the information for that job is returned;
     * Otherwise, information of all the jobs kept in table master will be returned.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.table.GetTransformJobInfoPResponse> getTransformJobInfo(
        alluxio.grpc.table.GetTransformJobInfoPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTransformJobInfoMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_ALL_DATABASES = 0;
  private static final int METHODID_GET_ALL_TABLES = 1;
  private static final int METHODID_GET_DATABASE = 2;
  private static final int METHODID_GET_TABLE = 3;
  private static final int METHODID_ATTACH_DATABASE = 4;
  private static final int METHODID_DETACH_DATABASE = 5;
  private static final int METHODID_SYNC_DATABASE = 6;
  private static final int METHODID_GET_TABLE_COLUMN_STATISTICS = 7;
  private static final int METHODID_GET_PARTITION_COLUMN_STATISTICS = 8;
  private static final int METHODID_READ_TABLE = 9;
  private static final int METHODID_TRANSFORM_TABLE = 10;
  private static final int METHODID_GET_TRANSFORM_JOB_INFO = 11;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final TableMasterClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(TableMasterClientServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_ALL_DATABASES:
          serviceImpl.getAllDatabases((alluxio.grpc.table.GetAllDatabasesPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.table.GetAllDatabasesPResponse>) responseObserver);
          break;
        case METHODID_GET_ALL_TABLES:
          serviceImpl.getAllTables((alluxio.grpc.table.GetAllTablesPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.table.GetAllTablesPResponse>) responseObserver);
          break;
        case METHODID_GET_DATABASE:
          serviceImpl.getDatabase((alluxio.grpc.table.GetDatabasePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.table.GetDatabasePResponse>) responseObserver);
          break;
        case METHODID_GET_TABLE:
          serviceImpl.getTable((alluxio.grpc.table.GetTablePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.table.GetTablePResponse>) responseObserver);
          break;
        case METHODID_ATTACH_DATABASE:
          serviceImpl.attachDatabase((alluxio.grpc.table.AttachDatabasePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.table.AttachDatabasePResponse>) responseObserver);
          break;
        case METHODID_DETACH_DATABASE:
          serviceImpl.detachDatabase((alluxio.grpc.table.DetachDatabasePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.table.DetachDatabasePResponse>) responseObserver);
          break;
        case METHODID_SYNC_DATABASE:
          serviceImpl.syncDatabase((alluxio.grpc.table.SyncDatabasePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.table.SyncDatabasePResponse>) responseObserver);
          break;
        case METHODID_GET_TABLE_COLUMN_STATISTICS:
          serviceImpl.getTableColumnStatistics((alluxio.grpc.table.GetTableColumnStatisticsPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.table.GetTableColumnStatisticsPResponse>) responseObserver);
          break;
        case METHODID_GET_PARTITION_COLUMN_STATISTICS:
          serviceImpl.getPartitionColumnStatistics((alluxio.grpc.table.GetPartitionColumnStatisticsPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.table.GetPartitionColumnStatisticsPResponse>) responseObserver);
          break;
        case METHODID_READ_TABLE:
          serviceImpl.readTable((alluxio.grpc.table.ReadTablePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.table.ReadTablePResponse>) responseObserver);
          break;
        case METHODID_TRANSFORM_TABLE:
          serviceImpl.transformTable((alluxio.grpc.table.TransformTablePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.table.TransformTablePResponse>) responseObserver);
          break;
        case METHODID_GET_TRANSFORM_JOB_INFO:
          serviceImpl.getTransformJobInfo((alluxio.grpc.table.GetTransformJobInfoPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.table.GetTransformJobInfoPResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class TableMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TableMasterClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.table.TableMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TableMasterClientService");
    }
  }

  private static final class TableMasterClientServiceFileDescriptorSupplier
      extends TableMasterClientServiceBaseDescriptorSupplier {
    TableMasterClientServiceFileDescriptorSupplier() {}
  }

  private static final class TableMasterClientServiceMethodDescriptorSupplier
      extends TableMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    TableMasterClientServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (TableMasterClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TableMasterClientServiceFileDescriptorSupplier())
              .addMethod(getGetAllDatabasesMethod())
              .addMethod(getGetAllTablesMethod())
              .addMethod(getGetDatabaseMethod())
              .addMethod(getGetTableMethod())
              .addMethod(getAttachDatabaseMethod())
              .addMethod(getDetachDatabaseMethod())
              .addMethod(getSyncDatabaseMethod())
              .addMethod(getGetTableColumnStatisticsMethod())
              .addMethod(getGetPartitionColumnStatisticsMethod())
              .addMethod(getReadTableMethod())
              .addMethod(getTransformTableMethod())
              .addMethod(getGetTransformJobInfoMethod())
              .build();
        }
      }
    }
    return result;
  }
}
