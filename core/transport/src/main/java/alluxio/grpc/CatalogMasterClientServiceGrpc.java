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

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.LoadTablePRequest,
      alluxio.grpc.LoadTablePResponse> getLoadTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "LoadTable",
      requestType = alluxio.grpc.LoadTablePRequest.class,
      responseType = alluxio.grpc.LoadTablePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.LoadTablePRequest,
      alluxio.grpc.LoadTablePResponse> getLoadTableMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.LoadTablePRequest, alluxio.grpc.LoadTablePResponse> getLoadTableMethod;
    if ((getLoadTableMethod = CatalogMasterClientServiceGrpc.getLoadTableMethod) == null) {
      synchronized (CatalogMasterClientServiceGrpc.class) {
        if ((getLoadTableMethod = CatalogMasterClientServiceGrpc.getLoadTableMethod) == null) {
          CatalogMasterClientServiceGrpc.getLoadTableMethod = getLoadTableMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.LoadTablePRequest, alluxio.grpc.LoadTablePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.CatalogMasterClientService", "LoadTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.LoadTablePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.LoadTablePResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new CatalogMasterClientServiceMethodDescriptorSupplier("LoadTable"))
                  .build();
          }
        }
     }
     return getLoadTableMethod;
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
     * Load a data table into the metastore
     * </pre>
     */
    public void loadTable(alluxio.grpc.LoadTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.LoadTablePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getLoadTableMethod(), responseObserver);
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
            getGetTableMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetTablePRequest,
                alluxio.grpc.GetTablePResponse>(
                  this, METHODID_GET_TABLE)))
          .addMethod(
            getLoadTableMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.LoadTablePRequest,
                alluxio.grpc.LoadTablePResponse>(
                  this, METHODID_LOAD_TABLE)))
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
     * Load a data table into the metastore
     * </pre>
     */
    public void loadTable(alluxio.grpc.LoadTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.LoadTablePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getLoadTableMethod(), getCallOptions()), request, responseObserver);
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
     * Load a data table into the metastore
     * </pre>
     */
    public alluxio.grpc.LoadTablePResponse loadTable(alluxio.grpc.LoadTablePRequest request) {
      return blockingUnaryCall(
          getChannel(), getLoadTableMethod(), getCallOptions(), request);
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
     * Load a data table into the metastore
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.LoadTablePResponse> loadTable(
        alluxio.grpc.LoadTablePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getLoadTableMethod(), getCallOptions()), request);
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
  }

  private static final int METHODID_GET_ALL_DATABASES = 0;
  private static final int METHODID_GET_ALL_TABLES = 1;
  private static final int METHODID_GET_TABLE = 2;
  private static final int METHODID_LOAD_TABLE = 3;
  private static final int METHODID_CREATE_TABLE = 4;
  private static final int METHODID_CREATE_DATABASE = 5;

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
        case METHODID_GET_TABLE:
          serviceImpl.getTable((alluxio.grpc.GetTablePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetTablePResponse>) responseObserver);
          break;
        case METHODID_LOAD_TABLE:
          serviceImpl.loadTable((alluxio.grpc.LoadTablePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.LoadTablePResponse>) responseObserver);
          break;
        case METHODID_CREATE_TABLE:
          serviceImpl.createTable((alluxio.grpc.CreateTablePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CreateTablePResponse>) responseObserver);
          break;
        case METHODID_CREATE_DATABASE:
          serviceImpl.createDatabase((alluxio.grpc.CreateDatabasePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CreateDatabasePResponse>) responseObserver);
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
              .addMethod(getGetTableMethod())
              .addMethod(getLoadTableMethod())
              .addMethod(getCreateTableMethod())
              .addMethod(getCreateDatabaseMethod())
              .build();
        }
      }
    }
    return result;
  }
}
