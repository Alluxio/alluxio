package alluxio.grpc.catalog;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 * <pre>
 **
 * This interface contains catalog master service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.17.1)",
    comments = "Source: grpc/catalog/catalog_master.proto")
public final class CatalogMasterClientServiceGrpc {

  private CatalogMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.catalog.CatalogMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetAllDatabasesPRequest,
      alluxio.grpc.catalog.GetAllDatabasesPResponse> getGetAllDatabasesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAllDatabases",
      requestType = alluxio.grpc.catalog.GetAllDatabasesPRequest.class,
      responseType = alluxio.grpc.catalog.GetAllDatabasesPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetAllDatabasesPRequest,
      alluxio.grpc.catalog.GetAllDatabasesPResponse> getGetAllDatabasesMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetAllDatabasesPRequest, alluxio.grpc.catalog.GetAllDatabasesPResponse> getGetAllDatabasesMethod;
    if ((getGetAllDatabasesMethod = CatalogMasterClientServiceGrpc.getGetAllDatabasesMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getGetAllDatabasesMethod = CatalogMasterClientServiceGrpc.getGetAllDatabasesMethod) == null) {
          CatalogMasterClientServiceGrpc.getGetAllDatabasesMethod = getGetAllDatabasesMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.catalog.GetAllDatabasesPRequest, alluxio.grpc.catalog.GetAllDatabasesPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.catalog.CatalogMasterClientService", "GetAllDatabases"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.GetAllDatabasesPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.GetAllDatabasesPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("GetAllDatabases"))
                  .build();
          }
        }
     }
     return getGetAllDatabasesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetAllTablesPRequest,
      alluxio.grpc.catalog.GetAllTablesPResponse> getGetAllTablesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAllTables",
      requestType = alluxio.grpc.catalog.GetAllTablesPRequest.class,
      responseType = alluxio.grpc.catalog.GetAllTablesPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetAllTablesPRequest,
      alluxio.grpc.catalog.GetAllTablesPResponse> getGetAllTablesMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetAllTablesPRequest, alluxio.grpc.catalog.GetAllTablesPResponse> getGetAllTablesMethod;
    if ((getGetAllTablesMethod = CatalogMasterClientServiceGrpc.getGetAllTablesMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getGetAllTablesMethod = CatalogMasterClientServiceGrpc.getGetAllTablesMethod) == null) {
          CatalogMasterClientServiceGrpc.getGetAllTablesMethod = getGetAllTablesMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.catalog.GetAllTablesPRequest, alluxio.grpc.catalog.GetAllTablesPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.catalog.CatalogMasterClientService", "GetAllTables"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.GetAllTablesPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.GetAllTablesPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("GetAllTables"))
                  .build();
          }
        }
     }
     return getGetAllTablesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetDatabasePRequest,
      alluxio.grpc.catalog.GetDatabasePResponse> getGetDatabaseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetDatabase",
      requestType = alluxio.grpc.catalog.GetDatabasePRequest.class,
      responseType = alluxio.grpc.catalog.GetDatabasePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetDatabasePRequest,
      alluxio.grpc.catalog.GetDatabasePResponse> getGetDatabaseMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetDatabasePRequest, alluxio.grpc.catalog.GetDatabasePResponse> getGetDatabaseMethod;
    if ((getGetDatabaseMethod = CatalogMasterClientServiceGrpc.getGetDatabaseMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getGetDatabaseMethod = CatalogMasterClientServiceGrpc.getGetDatabaseMethod) == null) {
          CatalogMasterClientServiceGrpc.getGetDatabaseMethod = getGetDatabaseMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.catalog.GetDatabasePRequest, alluxio.grpc.catalog.GetDatabasePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.catalog.CatalogMasterClientService", "GetDatabase"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.GetDatabasePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.GetDatabasePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("GetDatabase"))
                  .build();
          }
        }
     }
     return getGetDatabaseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetTablePRequest,
      alluxio.grpc.catalog.GetTablePResponse> getGetTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTable",
      requestType = alluxio.grpc.catalog.GetTablePRequest.class,
      responseType = alluxio.grpc.catalog.GetTablePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetTablePRequest,
      alluxio.grpc.catalog.GetTablePResponse> getGetTableMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetTablePRequest, alluxio.grpc.catalog.GetTablePResponse> getGetTableMethod;
    if ((getGetTableMethod = CatalogMasterClientServiceGrpc.getGetTableMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getGetTableMethod = CatalogMasterClientServiceGrpc.getGetTableMethod) == null) {
          CatalogMasterClientServiceGrpc.getGetTableMethod = getGetTableMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.catalog.GetTablePRequest, alluxio.grpc.catalog.GetTablePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.catalog.CatalogMasterClientService", "GetTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.GetTablePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.GetTablePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("GetTable"))
                  .build();
          }
        }
     }
     return getGetTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.catalog.AttachDatabasePRequest,
      alluxio.grpc.catalog.AttachDatabasePResponse> getAttachDatabaseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AttachDatabase",
      requestType = alluxio.grpc.catalog.AttachDatabasePRequest.class,
      responseType = alluxio.grpc.catalog.AttachDatabasePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.catalog.AttachDatabasePRequest,
      alluxio.grpc.catalog.AttachDatabasePResponse> getAttachDatabaseMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.catalog.AttachDatabasePRequest, alluxio.grpc.catalog.AttachDatabasePResponse> getAttachDatabaseMethod;
    if ((getAttachDatabaseMethod = CatalogMasterClientServiceGrpc.getAttachDatabaseMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getAttachDatabaseMethod = CatalogMasterClientServiceGrpc.getAttachDatabaseMethod) == null) {
          CatalogMasterClientServiceGrpc.getAttachDatabaseMethod = getAttachDatabaseMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.catalog.AttachDatabasePRequest, alluxio.grpc.catalog.AttachDatabasePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.catalog.CatalogMasterClientService", "AttachDatabase"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.AttachDatabasePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.AttachDatabasePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("AttachDatabase"))
                  .build();
          }
        }
     }
     return getAttachDatabaseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetTableColumnStatisticsPRequest,
      alluxio.grpc.catalog.GetTableColumnStatisticsPResponse> getGetTableColumnStatisticsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTableColumnStatistics",
      requestType = alluxio.grpc.catalog.GetTableColumnStatisticsPRequest.class,
      responseType = alluxio.grpc.catalog.GetTableColumnStatisticsPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetTableColumnStatisticsPRequest,
      alluxio.grpc.catalog.GetTableColumnStatisticsPResponse> getGetTableColumnStatisticsMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetTableColumnStatisticsPRequest, alluxio.grpc.catalog.GetTableColumnStatisticsPResponse> getGetTableColumnStatisticsMethod;
    if ((getGetTableColumnStatisticsMethod = CatalogMasterClientServiceGrpc.getGetTableColumnStatisticsMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getGetTableColumnStatisticsMethod = CatalogMasterClientServiceGrpc.getGetTableColumnStatisticsMethod) == null) {
          CatalogMasterClientServiceGrpc.getGetTableColumnStatisticsMethod = getGetTableColumnStatisticsMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.catalog.GetTableColumnStatisticsPRequest, alluxio.grpc.catalog.GetTableColumnStatisticsPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.catalog.CatalogMasterClientService", "GetTableColumnStatistics"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.GetTableColumnStatisticsPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.GetTableColumnStatisticsPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("GetTableColumnStatistics"))
                  .build();
          }
        }
     }
     return getGetTableColumnStatisticsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetPartitionColumnStatisticsPRequest,
      alluxio.grpc.catalog.GetPartitionColumnStatisticsPResponse> getGetPartitionColumnStatisticsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetPartitionColumnStatistics",
      requestType = alluxio.grpc.catalog.GetPartitionColumnStatisticsPRequest.class,
      responseType = alluxio.grpc.catalog.GetPartitionColumnStatisticsPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetPartitionColumnStatisticsPRequest,
      alluxio.grpc.catalog.GetPartitionColumnStatisticsPResponse> getGetPartitionColumnStatisticsMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.catalog.GetPartitionColumnStatisticsPRequest, alluxio.grpc.catalog.GetPartitionColumnStatisticsPResponse> getGetPartitionColumnStatisticsMethod;
    if ((getGetPartitionColumnStatisticsMethod = CatalogMasterClientServiceGrpc.getGetPartitionColumnStatisticsMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getGetPartitionColumnStatisticsMethod = CatalogMasterClientServiceGrpc.getGetPartitionColumnStatisticsMethod) == null) {
          CatalogMasterClientServiceGrpc.getGetPartitionColumnStatisticsMethod = getGetPartitionColumnStatisticsMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.catalog.GetPartitionColumnStatisticsPRequest, alluxio.grpc.catalog.GetPartitionColumnStatisticsPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.catalog.CatalogMasterClientService", "GetPartitionColumnStatistics"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.GetPartitionColumnStatisticsPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.GetPartitionColumnStatisticsPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("GetPartitionColumnStatistics"))
                  .build();
          }
        }
     }
     return getGetPartitionColumnStatisticsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.catalog.ReadTablePRequest,
      alluxio.grpc.catalog.ReadTablePResponse> getReadTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReadTable",
      requestType = alluxio.grpc.catalog.ReadTablePRequest.class,
      responseType = alluxio.grpc.catalog.ReadTablePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.catalog.ReadTablePRequest,
      alluxio.grpc.catalog.ReadTablePResponse> getReadTableMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.catalog.ReadTablePRequest, alluxio.grpc.catalog.ReadTablePResponse> getReadTableMethod;
    if ((getReadTableMethod = CatalogMasterClientServiceGrpc.getReadTableMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getReadTableMethod = CatalogMasterClientServiceGrpc.getReadTableMethod) == null) {
          CatalogMasterClientServiceGrpc.getReadTableMethod = getReadTableMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.catalog.ReadTablePRequest, alluxio.grpc.catalog.ReadTablePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.catalog.CatalogMasterClientService", "ReadTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.ReadTablePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.catalog.ReadTablePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("ReadTable"))
                  .build();
          }
        }
     }
     return getReadTableMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static CatalogMasterClientServiceStub newStub(io.grpc.Channel channel) {
    return new CatalogMasterClientServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static CatalogMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new CatalogMasterClientServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static CatalogMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new CatalogMasterClientServiceFutureStub(channel);
  }

  /**
   * <pre>
   **
   * This interface contains catalog master service endpoints for Alluxio clients.
   * </pre>
   */
  public static abstract class CatalogMasterClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Returns all databases in the catalog
     * </pre>
     */
    public void getAllDatabases(alluxio.grpc.catalog.GetAllDatabasesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetAllDatabasesPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetAllDatabasesMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns all tables in the database
     * </pre>
     */
    public void getAllTables(alluxio.grpc.catalog.GetAllTablesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetAllTablesPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetAllTablesMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Gets a database by name from the catalog master
     * </pre>
     */
    public void getDatabase(alluxio.grpc.catalog.GetDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetDatabasePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetDatabaseMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a specific table info
     * </pre>
     */
    public void getTable(alluxio.grpc.catalog.GetTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetTablePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetTableMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Attach an existing database into the catalog as a new database name
     * </pre>
     */
    public void attachDatabase(alluxio.grpc.catalog.AttachDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.AttachDatabasePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getAttachDatabaseMethod(), responseObserver);
    }

    /**
     */
    public void getTableColumnStatistics(alluxio.grpc.catalog.GetTableColumnStatisticsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetTableColumnStatisticsPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetTableColumnStatisticsMethod(), responseObserver);
    }

    /**
     */
    public void getPartitionColumnStatistics(alluxio.grpc.catalog.GetPartitionColumnStatisticsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetPartitionColumnStatisticsPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetPartitionColumnStatisticsMethod(), responseObserver);
    }

    /**
     */
    public void readTable(alluxio.grpc.catalog.ReadTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.ReadTablePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getReadTableMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetAllDatabasesMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.catalog.GetAllDatabasesPRequest,
                alluxio.grpc.catalog.GetAllDatabasesPResponse>(
                  this, METHODID_GET_ALL_DATABASES)))
          .addMethod(
            getGetAllTablesMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.catalog.GetAllTablesPRequest,
                alluxio.grpc.catalog.GetAllTablesPResponse>(
                  this, METHODID_GET_ALL_TABLES)))
          .addMethod(
            getGetDatabaseMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.catalog.GetDatabasePRequest,
                alluxio.grpc.catalog.GetDatabasePResponse>(
                  this, METHODID_GET_DATABASE)))
          .addMethod(
            getGetTableMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.catalog.GetTablePRequest,
                alluxio.grpc.catalog.GetTablePResponse>(
                  this, METHODID_GET_TABLE)))
          .addMethod(
            getAttachDatabaseMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.catalog.AttachDatabasePRequest,
                alluxio.grpc.catalog.AttachDatabasePResponse>(
                  this, METHODID_ATTACH_DATABASE)))
          .addMethod(
            getGetTableColumnStatisticsMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.catalog.GetTableColumnStatisticsPRequest,
                alluxio.grpc.catalog.GetTableColumnStatisticsPResponse>(
                  this, METHODID_GET_TABLE_COLUMN_STATISTICS)))
          .addMethod(
            getGetPartitionColumnStatisticsMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.catalog.GetPartitionColumnStatisticsPRequest,
                alluxio.grpc.catalog.GetPartitionColumnStatisticsPResponse>(
                  this, METHODID_GET_PARTITION_COLUMN_STATISTICS)))
          .addMethod(
            getReadTableMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.catalog.ReadTablePRequest,
                alluxio.grpc.catalog.ReadTablePResponse>(
                  this, METHODID_READ_TABLE)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains catalog master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class CatalogMasterClientServiceStub extends io.grpc.stub.AbstractStub<CatalogMasterClientServiceStub> {
    private CatalogMasterClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CatalogMasterClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CatalogMasterClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CatalogMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns all databases in the catalog
     * </pre>
     */
    public void getAllDatabases(alluxio.grpc.catalog.GetAllDatabasesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetAllDatabasesPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetAllDatabasesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns all tables in the database
     * </pre>
     */
    public void getAllTables(alluxio.grpc.catalog.GetAllTablesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetAllTablesPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetAllTablesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Gets a database by name from the catalog master
     * </pre>
     */
    public void getDatabase(alluxio.grpc.catalog.GetDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetDatabasePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetDatabaseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a specific table info
     * </pre>
     */
    public void getTable(alluxio.grpc.catalog.GetTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetTablePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Attach an existing database into the catalog as a new database name
     * </pre>
     */
    public void attachDatabase(alluxio.grpc.catalog.AttachDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.AttachDatabasePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getAttachDatabaseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getTableColumnStatistics(alluxio.grpc.catalog.GetTableColumnStatisticsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetTableColumnStatisticsPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetTableColumnStatisticsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getPartitionColumnStatistics(alluxio.grpc.catalog.GetPartitionColumnStatisticsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetPartitionColumnStatisticsPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetPartitionColumnStatisticsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void readTable(alluxio.grpc.catalog.ReadTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.catalog.ReadTablePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getReadTableMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains catalog master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class CatalogMasterClientServiceBlockingStub extends io.grpc.stub.AbstractStub<CatalogMasterClientServiceBlockingStub> {
    private CatalogMasterClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CatalogMasterClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CatalogMasterClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CatalogMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns all databases in the catalog
     * </pre>
     */
    public alluxio.grpc.catalog.GetAllDatabasesPResponse getAllDatabases(alluxio.grpc.catalog.GetAllDatabasesPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetAllDatabasesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns all tables in the database
     * </pre>
     */
    public alluxio.grpc.catalog.GetAllTablesPResponse getAllTables(alluxio.grpc.catalog.GetAllTablesPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetAllTablesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Gets a database by name from the catalog master
     * </pre>
     */
    public alluxio.grpc.catalog.GetDatabasePResponse getDatabase(alluxio.grpc.catalog.GetDatabasePRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetDatabaseMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns a specific table info
     * </pre>
     */
    public alluxio.grpc.catalog.GetTablePResponse getTable(alluxio.grpc.catalog.GetTablePRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetTableMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Attach an existing database into the catalog as a new database name
     * </pre>
     */
    public alluxio.grpc.catalog.AttachDatabasePResponse attachDatabase(alluxio.grpc.catalog.AttachDatabasePRequest request) {
      return blockingUnaryCall(
          getChannel(), getAttachDatabaseMethod(), getCallOptions(), request);
    }

    /**
     */
    public alluxio.grpc.catalog.GetTableColumnStatisticsPResponse getTableColumnStatistics(alluxio.grpc.catalog.GetTableColumnStatisticsPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetTableColumnStatisticsMethod(), getCallOptions(), request);
    }

    /**
     */
    public alluxio.grpc.catalog.GetPartitionColumnStatisticsPResponse getPartitionColumnStatistics(alluxio.grpc.catalog.GetPartitionColumnStatisticsPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetPartitionColumnStatisticsMethod(), getCallOptions(), request);
    }

    /**
     */
    public alluxio.grpc.catalog.ReadTablePResponse readTable(alluxio.grpc.catalog.ReadTablePRequest request) {
      return blockingUnaryCall(
          getChannel(), getReadTableMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains catalog master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class CatalogMasterClientServiceFutureStub extends io.grpc.stub.AbstractStub<CatalogMasterClientServiceFutureStub> {
    private CatalogMasterClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private CatalogMasterClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected CatalogMasterClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new CatalogMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Returns all databases in the catalog
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.catalog.GetAllDatabasesPResponse> getAllDatabases(
        alluxio.grpc.catalog.GetAllDatabasesPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetAllDatabasesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns all tables in the database
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.catalog.GetAllTablesPResponse> getAllTables(
        alluxio.grpc.catalog.GetAllTablesPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetAllTablesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Gets a database by name from the catalog master
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.catalog.GetDatabasePResponse> getDatabase(
        alluxio.grpc.catalog.GetDatabasePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetDatabaseMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns a specific table info
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.catalog.GetTablePResponse> getTable(
        alluxio.grpc.catalog.GetTablePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetTableMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Attach an existing database into the catalog as a new database name
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.catalog.AttachDatabasePResponse> attachDatabase(
        alluxio.grpc.catalog.AttachDatabasePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getAttachDatabaseMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.catalog.GetTableColumnStatisticsPResponse> getTableColumnStatistics(
        alluxio.grpc.catalog.GetTableColumnStatisticsPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetTableColumnStatisticsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.catalog.GetPartitionColumnStatisticsPResponse> getPartitionColumnStatistics(
        alluxio.grpc.catalog.GetPartitionColumnStatisticsPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetPartitionColumnStatisticsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.catalog.ReadTablePResponse> readTable(
        alluxio.grpc.catalog.ReadTablePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getReadTableMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_ALL_DATABASES = 0;
  private static final int METHODID_GET_ALL_TABLES = 1;
  private static final int METHODID_GET_DATABASE = 2;
  private static final int METHODID_GET_TABLE = 3;
  private static final int METHODID_ATTACH_DATABASE = 4;
  private static final int METHODID_GET_TABLE_COLUMN_STATISTICS = 5;
  private static final int METHODID_GET_PARTITION_COLUMN_STATISTICS = 6;
  private static final int METHODID_READ_TABLE = 7;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final CatalogMasterClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(CatalogMasterClientServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_ALL_DATABASES:
          serviceImpl.getAllDatabases((alluxio.grpc.catalog.GetAllDatabasesPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetAllDatabasesPResponse>) responseObserver);
          break;
        case METHODID_GET_ALL_TABLES:
          serviceImpl.getAllTables((alluxio.grpc.catalog.GetAllTablesPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetAllTablesPResponse>) responseObserver);
          break;
        case METHODID_GET_DATABASE:
          serviceImpl.getDatabase((alluxio.grpc.catalog.GetDatabasePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetDatabasePResponse>) responseObserver);
          break;
        case METHODID_GET_TABLE:
          serviceImpl.getTable((alluxio.grpc.catalog.GetTablePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetTablePResponse>) responseObserver);
          break;
        case METHODID_ATTACH_DATABASE:
          serviceImpl.attachDatabase((alluxio.grpc.catalog.AttachDatabasePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.catalog.AttachDatabasePResponse>) responseObserver);
          break;
        case METHODID_GET_TABLE_COLUMN_STATISTICS:
          serviceImpl.getTableColumnStatistics((alluxio.grpc.catalog.GetTableColumnStatisticsPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetTableColumnStatisticsPResponse>) responseObserver);
          break;
        case METHODID_GET_PARTITION_COLUMN_STATISTICS:
          serviceImpl.getPartitionColumnStatistics((alluxio.grpc.catalog.GetPartitionColumnStatisticsPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.catalog.GetPartitionColumnStatisticsPResponse>) responseObserver);
          break;
        case METHODID_READ_TABLE:
          serviceImpl.readTable((alluxio.grpc.catalog.ReadTablePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.catalog.ReadTablePResponse>) responseObserver);
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

  private static abstract class CatalogMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    CatalogMasterClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.catalog.CatalogMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("CatalogMasterClientService");
    }
  }

  private static final class CatalogMasterClientServiceFileDescriptorSupplier
      extends CatalogMasterClientServiceBaseDescriptorSupplier {
    CatalogMasterClientServiceFileDescriptorSupplier() {}
  }

  private static final class CatalogMasterClientServiceMethodDescriptorSupplier
      extends CatalogMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    CatalogMasterClientServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (CatalogMasterClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new CatalogMasterClientServiceFileDescriptorSupplier())
              .addMethod(getGetAllDatabasesMethod())
              .addMethod(getGetAllTablesMethod())
              .addMethod(getGetDatabaseMethod())
              .addMethod(getGetTableMethod())
              .addMethod(getAttachDatabaseMethod())
              .addMethod(getGetTableColumnStatisticsMethod())
              .addMethod(getGetPartitionColumnStatisticsMethod())
              .addMethod(getReadTableMethod())
              .build();
        }
      }
    }
    return result;
  }
}
