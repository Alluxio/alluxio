package alluxio.grpc;

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
    comments = "Source: grpc/catalog_master.proto")
public final class CatalogMasterClientServiceGrpc {

  private CatalogMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.CatalogMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetAllDatabasesPRequest,
      alluxio.grpc.GetAllDatabasesPResponse> getGetAllDatabasesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAllDatabases",
      requestType = alluxio.grpc.GetAllDatabasesPRequest.class,
      responseType = alluxio.grpc.GetAllDatabasesPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetAllDatabasesPRequest,
      alluxio.grpc.GetAllDatabasesPResponse> getGetAllDatabasesMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetAllDatabasesPRequest, alluxio.grpc.GetAllDatabasesPResponse> getGetAllDatabasesMethod;
    if ((getGetAllDatabasesMethod = CatalogMasterClientServiceGrpc.getGetAllDatabasesMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getGetAllDatabasesMethod = CatalogMasterClientServiceGrpc.getGetAllDatabasesMethod) == null) {
          CatalogMasterClientServiceGrpc.getGetAllDatabasesMethod = getGetAllDatabasesMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetAllDatabasesPRequest, alluxio.grpc.GetAllDatabasesPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.CatalogMasterClientService", "GetAllDatabases"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetAllDatabasesPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetAllDatabasesPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("GetAllDatabases"))
                  .build();
          }
        }
     }
     return getGetAllDatabasesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetAllTablesPRequest,
      alluxio.grpc.GetAllTablesPResponse> getGetAllTablesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetAllTables",
      requestType = alluxio.grpc.GetAllTablesPRequest.class,
      responseType = alluxio.grpc.GetAllTablesPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetAllTablesPRequest,
      alluxio.grpc.GetAllTablesPResponse> getGetAllTablesMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetAllTablesPRequest, alluxio.grpc.GetAllTablesPResponse> getGetAllTablesMethod;
    if ((getGetAllTablesMethod = CatalogMasterClientServiceGrpc.getGetAllTablesMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getGetAllTablesMethod = CatalogMasterClientServiceGrpc.getGetAllTablesMethod) == null) {
          CatalogMasterClientServiceGrpc.getGetAllTablesMethod = getGetAllTablesMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetAllTablesPRequest, alluxio.grpc.GetAllTablesPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.CatalogMasterClientService", "GetAllTables"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetAllTablesPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetAllTablesPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("GetAllTables"))
                  .build();
          }
        }
     }
     return getGetAllTablesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetDatabasePRequest,
      alluxio.grpc.GetDatabasePResponse> getGetDatabaseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetDatabase",
      requestType = alluxio.grpc.GetDatabasePRequest.class,
      responseType = alluxio.grpc.GetDatabasePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetDatabasePRequest,
      alluxio.grpc.GetDatabasePResponse> getGetDatabaseMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetDatabasePRequest, alluxio.grpc.GetDatabasePResponse> getGetDatabaseMethod;
    if ((getGetDatabaseMethod = CatalogMasterClientServiceGrpc.getGetDatabaseMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getGetDatabaseMethod = CatalogMasterClientServiceGrpc.getGetDatabaseMethod) == null) {
          CatalogMasterClientServiceGrpc.getGetDatabaseMethod = getGetDatabaseMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetDatabasePRequest, alluxio.grpc.GetDatabasePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.CatalogMasterClientService", "GetDatabase"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetDatabasePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetDatabasePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("GetDatabase"))
                  .build();
          }
        }
     }
     return getGetDatabaseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetTablePRequest,
      alluxio.grpc.GetTablePResponse> getGetTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTable",
      requestType = alluxio.grpc.GetTablePRequest.class,
      responseType = alluxio.grpc.GetTablePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetTablePRequest,
      alluxio.grpc.GetTablePResponse> getGetTableMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetTablePRequest, alluxio.grpc.GetTablePResponse> getGetTableMethod;
    if ((getGetTableMethod = CatalogMasterClientServiceGrpc.getGetTableMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getGetTableMethod = CatalogMasterClientServiceGrpc.getGetTableMethod) == null) {
          CatalogMasterClientServiceGrpc.getGetTableMethod = getGetTableMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetTablePRequest, alluxio.grpc.GetTablePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.CatalogMasterClientService", "GetTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetTablePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetTablePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("GetTable"))
                  .build();
          }
        }
     }
     return getGetTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CreateTablePRequest,
      alluxio.grpc.CreateTablePResponse> getCreateTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateTable",
      requestType = alluxio.grpc.CreateTablePRequest.class,
      responseType = alluxio.grpc.CreateTablePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CreateTablePRequest,
      alluxio.grpc.CreateTablePResponse> getCreateTableMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CreateTablePRequest, alluxio.grpc.CreateTablePResponse> getCreateTableMethod;
    if ((getCreateTableMethod = CatalogMasterClientServiceGrpc.getCreateTableMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getCreateTableMethod = CatalogMasterClientServiceGrpc.getCreateTableMethod) == null) {
          CatalogMasterClientServiceGrpc.getCreateTableMethod = getCreateTableMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.CreateTablePRequest, alluxio.grpc.CreateTablePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.CatalogMasterClientService", "CreateTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CreateTablePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CreateTablePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("CreateTable"))
                  .build();
          }
        }
     }
     return getCreateTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CreateDatabasePRequest,
      alluxio.grpc.CreateDatabasePResponse> getCreateDatabaseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateDatabase",
      requestType = alluxio.grpc.CreateDatabasePRequest.class,
      responseType = alluxio.grpc.CreateDatabasePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CreateDatabasePRequest,
      alluxio.grpc.CreateDatabasePResponse> getCreateDatabaseMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CreateDatabasePRequest, alluxio.grpc.CreateDatabasePResponse> getCreateDatabaseMethod;
    if ((getCreateDatabaseMethod = CatalogMasterClientServiceGrpc.getCreateDatabaseMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getCreateDatabaseMethod = CatalogMasterClientServiceGrpc.getCreateDatabaseMethod) == null) {
          CatalogMasterClientServiceGrpc.getCreateDatabaseMethod = getCreateDatabaseMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.CreateDatabasePRequest, alluxio.grpc.CreateDatabasePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.CatalogMasterClientService", "CreateDatabase"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CreateDatabasePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CreateDatabasePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("CreateDatabase"))
                  .build();
          }
        }
     }
     return getCreateDatabaseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.AttachDatabasePRequest,
      alluxio.grpc.AttachDatabasePResponse> getAttachDatabaseMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AttachDatabase",
      requestType = alluxio.grpc.AttachDatabasePRequest.class,
      responseType = alluxio.grpc.AttachDatabasePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.AttachDatabasePRequest,
      alluxio.grpc.AttachDatabasePResponse> getAttachDatabaseMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.AttachDatabasePRequest, alluxio.grpc.AttachDatabasePResponse> getAttachDatabaseMethod;
    if ((getAttachDatabaseMethod = CatalogMasterClientServiceGrpc.getAttachDatabaseMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getAttachDatabaseMethod = CatalogMasterClientServiceGrpc.getAttachDatabaseMethod) == null) {
          CatalogMasterClientServiceGrpc.getAttachDatabaseMethod = getAttachDatabaseMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.AttachDatabasePRequest, alluxio.grpc.AttachDatabasePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.CatalogMasterClientService", "AttachDatabase"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.AttachDatabasePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.AttachDatabasePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("AttachDatabase"))
                  .build();
          }
        }
     }
     return getAttachDatabaseMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetStatisticsPRequest,
      alluxio.grpc.GetStatisticsPResponse> getGetStatisticsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetStatistics",
      requestType = alluxio.grpc.GetStatisticsPRequest.class,
      responseType = alluxio.grpc.GetStatisticsPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetStatisticsPRequest,
      alluxio.grpc.GetStatisticsPResponse> getGetStatisticsMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetStatisticsPRequest, alluxio.grpc.GetStatisticsPResponse> getGetStatisticsMethod;
    if ((getGetStatisticsMethod = CatalogMasterClientServiceGrpc.getGetStatisticsMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getGetStatisticsMethod = CatalogMasterClientServiceGrpc.getGetStatisticsMethod) == null) {
          CatalogMasterClientServiceGrpc.getGetStatisticsMethod = getGetStatisticsMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.GetStatisticsPRequest, alluxio.grpc.GetStatisticsPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.CatalogMasterClientService", "GetStatistics"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetStatisticsPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetStatisticsPResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("GetStatistics"))
                  .build();
          }
        }
     }
     return getGetStatisticsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.ReadTablePRequest,
      alluxio.grpc.ReadTablePResponse> getReadTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReadTable",
      requestType = alluxio.grpc.ReadTablePRequest.class,
      responseType = alluxio.grpc.ReadTablePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.ReadTablePRequest,
      alluxio.grpc.ReadTablePResponse> getReadTableMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.ReadTablePRequest, alluxio.grpc.ReadTablePResponse> getReadTableMethod;
    if ((getReadTableMethod = CatalogMasterClientServiceGrpc.getReadTableMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getReadTableMethod = CatalogMasterClientServiceGrpc.getReadTableMethod) == null) {
          CatalogMasterClientServiceGrpc.getReadTableMethod = getReadTableMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.ReadTablePRequest, alluxio.grpc.ReadTablePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.CatalogMasterClientService", "ReadTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ReadTablePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ReadTablePResponse.getDefaultInstance()))
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
    public void getAllDatabases(alluxio.grpc.GetAllDatabasesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetAllDatabasesPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetAllDatabasesMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns all tables in the database
     * </pre>
     */
    public void getAllTables(alluxio.grpc.GetAllTablesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetAllTablesPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetAllTablesMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Gets a database by name from the catalog master
     * </pre>
     */
    public void getDatabase(alluxio.grpc.GetDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetDatabasePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetDatabaseMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a specific table info
     * </pre>
     */
    public void getTable(alluxio.grpc.GetTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetTablePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetTableMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Create a new table in the metastore
     * </pre>
     */
    public void createTable(alluxio.grpc.CreateTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateTablePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCreateTableMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Create a new database in the metastore
     * </pre>
     */
    public void createDatabase(alluxio.grpc.CreateDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateDatabasePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCreateDatabaseMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Attach an existing database into the catalog as a new database name
     * </pre>
     */
    public void attachDatabase(alluxio.grpc.AttachDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.AttachDatabasePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getAttachDatabaseMethod(), responseObserver);
    }

    /**
     */
    public void getStatistics(alluxio.grpc.GetStatisticsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetStatisticsPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetStatisticsMethod(), responseObserver);
    }

    /**
     */
    public void readTable(alluxio.grpc.ReadTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ReadTablePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getReadTableMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetAllDatabasesMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetAllDatabasesPRequest,
                alluxio.grpc.GetAllDatabasesPResponse>(
                  this, METHODID_GET_ALL_DATABASES)))
          .addMethod(
            getGetAllTablesMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetAllTablesPRequest,
                alluxio.grpc.GetAllTablesPResponse>(
                  this, METHODID_GET_ALL_TABLES)))
          .addMethod(
            getGetDatabaseMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetDatabasePRequest,
                alluxio.grpc.GetDatabasePResponse>(
                  this, METHODID_GET_DATABASE)))
          .addMethod(
            getGetTableMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetTablePRequest,
                alluxio.grpc.GetTablePResponse>(
                  this, METHODID_GET_TABLE)))
          .addMethod(
            getCreateTableMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CreateTablePRequest,
                alluxio.grpc.CreateTablePResponse>(
                  this, METHODID_CREATE_TABLE)))
          .addMethod(
            getCreateDatabaseMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CreateDatabasePRequest,
                alluxio.grpc.CreateDatabasePResponse>(
                  this, METHODID_CREATE_DATABASE)))
          .addMethod(
            getAttachDatabaseMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.AttachDatabasePRequest,
                alluxio.grpc.AttachDatabasePResponse>(
                  this, METHODID_ATTACH_DATABASE)))
          .addMethod(
            getGetStatisticsMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetStatisticsPRequest,
                alluxio.grpc.GetStatisticsPResponse>(
                  this, METHODID_GET_STATISTICS)))
          .addMethod(
            getReadTableMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.ReadTablePRequest,
                alluxio.grpc.ReadTablePResponse>(
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
    public void getAllDatabases(alluxio.grpc.GetAllDatabasesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetAllDatabasesPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetAllDatabasesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns all tables in the database
     * </pre>
     */
    public void getAllTables(alluxio.grpc.GetAllTablesPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetAllTablesPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetAllTablesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Gets a database by name from the catalog master
     * </pre>
     */
    public void getDatabase(alluxio.grpc.GetDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetDatabasePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetDatabaseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a specific table info
     * </pre>
     */
    public void getTable(alluxio.grpc.GetTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetTablePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Create a new table in the metastore
     * </pre>
     */
    public void createTable(alluxio.grpc.CreateTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateTablePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCreateTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Create a new database in the metastore
     * </pre>
     */
    public void createDatabase(alluxio.grpc.CreateDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateDatabasePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCreateDatabaseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Attach an existing database into the catalog as a new database name
     * </pre>
     */
    public void attachDatabase(alluxio.grpc.AttachDatabasePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.AttachDatabasePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getAttachDatabaseMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getStatistics(alluxio.grpc.GetStatisticsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetStatisticsPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetStatisticsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void readTable(alluxio.grpc.ReadTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ReadTablePResponse> responseObserver) {
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
    public alluxio.grpc.GetAllDatabasesPResponse getAllDatabases(alluxio.grpc.GetAllDatabasesPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetAllDatabasesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns all tables in the database
     * </pre>
     */
    public alluxio.grpc.GetAllTablesPResponse getAllTables(alluxio.grpc.GetAllTablesPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetAllTablesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Gets a database by name from the catalog master
     * </pre>
     */
    public alluxio.grpc.GetDatabasePResponse getDatabase(alluxio.grpc.GetDatabasePRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetDatabaseMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns a specific table info
     * </pre>
     */
    public alluxio.grpc.GetTablePResponse getTable(alluxio.grpc.GetTablePRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetTableMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Create a new table in the metastore
     * </pre>
     */
    public alluxio.grpc.CreateTablePResponse createTable(alluxio.grpc.CreateTablePRequest request) {
      return blockingUnaryCall(
          getChannel(), getCreateTableMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Create a new database in the metastore
     * </pre>
     */
    public alluxio.grpc.CreateDatabasePResponse createDatabase(alluxio.grpc.CreateDatabasePRequest request) {
      return blockingUnaryCall(
          getChannel(), getCreateDatabaseMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Attach an existing database into the catalog as a new database name
     * </pre>
     */
    public alluxio.grpc.AttachDatabasePResponse attachDatabase(alluxio.grpc.AttachDatabasePRequest request) {
      return blockingUnaryCall(
          getChannel(), getAttachDatabaseMethod(), getCallOptions(), request);
    }

    /**
     */
    public alluxio.grpc.GetStatisticsPResponse getStatistics(alluxio.grpc.GetStatisticsPRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetStatisticsMethod(), getCallOptions(), request);
    }

    /**
     */
    public alluxio.grpc.ReadTablePResponse readTable(alluxio.grpc.ReadTablePRequest request) {
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
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetAllDatabasesPResponse> getAllDatabases(
        alluxio.grpc.GetAllDatabasesPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetAllDatabasesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns all tables in the database
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetAllTablesPResponse> getAllTables(
        alluxio.grpc.GetAllTablesPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetAllTablesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Gets a database by name from the catalog master
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetDatabasePResponse> getDatabase(
        alluxio.grpc.GetDatabasePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetDatabaseMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns a specific table info
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetTablePResponse> getTable(
        alluxio.grpc.GetTablePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetTableMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Create a new table in the metastore
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CreateTablePResponse> createTable(
        alluxio.grpc.CreateTablePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCreateTableMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Create a new database in the metastore
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CreateDatabasePResponse> createDatabase(
        alluxio.grpc.CreateDatabasePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCreateDatabaseMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Attach an existing database into the catalog as a new database name
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.AttachDatabasePResponse> attachDatabase(
        alluxio.grpc.AttachDatabasePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getAttachDatabaseMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetStatisticsPResponse> getStatistics(
        alluxio.grpc.GetStatisticsPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetStatisticsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.ReadTablePResponse> readTable(
        alluxio.grpc.ReadTablePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getReadTableMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_ALL_DATABASES = 0;
  private static final int METHODID_GET_ALL_TABLES = 1;
  private static final int METHODID_GET_DATABASE = 2;
  private static final int METHODID_GET_TABLE = 3;
  private static final int METHODID_CREATE_TABLE = 4;
  private static final int METHODID_CREATE_DATABASE = 5;
  private static final int METHODID_ATTACH_DATABASE = 6;
  private static final int METHODID_GET_STATISTICS = 7;
  private static final int METHODID_READ_TABLE = 8;

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
          serviceImpl.getAllDatabases((alluxio.grpc.GetAllDatabasesPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetAllDatabasesPResponse>) responseObserver);
          break;
        case METHODID_GET_ALL_TABLES:
          serviceImpl.getAllTables((alluxio.grpc.GetAllTablesPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetAllTablesPResponse>) responseObserver);
          break;
        case METHODID_GET_DATABASE:
          serviceImpl.getDatabase((alluxio.grpc.GetDatabasePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetDatabasePResponse>) responseObserver);
          break;
        case METHODID_GET_TABLE:
          serviceImpl.getTable((alluxio.grpc.GetTablePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetTablePResponse>) responseObserver);
          break;
        case METHODID_CREATE_TABLE:
          serviceImpl.createTable((alluxio.grpc.CreateTablePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CreateTablePResponse>) responseObserver);
          break;
        case METHODID_CREATE_DATABASE:
          serviceImpl.createDatabase((alluxio.grpc.CreateDatabasePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CreateDatabasePResponse>) responseObserver);
          break;
        case METHODID_ATTACH_DATABASE:
          serviceImpl.attachDatabase((alluxio.grpc.AttachDatabasePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.AttachDatabasePResponse>) responseObserver);
          break;
        case METHODID_GET_STATISTICS:
          serviceImpl.getStatistics((alluxio.grpc.GetStatisticsPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetStatisticsPResponse>) responseObserver);
          break;
        case METHODID_READ_TABLE:
          serviceImpl.readTable((alluxio.grpc.ReadTablePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.ReadTablePResponse>) responseObserver);
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
      return alluxio.grpc.CatalogMasterProto.getDescriptor();
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
              .addMethod(getCreateTableMethod())
              .addMethod(getCreateDatabaseMethod())
              .addMethod(getAttachDatabaseMethod())
              .addMethod(getGetStatisticsMethod())
              .addMethod(getReadTableMethod())
              .build();
        }
      }
    }
    return result;
  }
}
