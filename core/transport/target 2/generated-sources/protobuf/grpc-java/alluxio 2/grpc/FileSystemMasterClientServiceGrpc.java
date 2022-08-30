package alluxio.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 **
 * This interface contains file system master service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.37.0)",
    comments = "Source: grpc/file_system_master.proto")
public final class FileSystemMasterClientServiceGrpc {

  private FileSystemMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.file.FileSystemMasterClientService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CheckAccessPRequest,
      alluxio.grpc.CheckAccessPResponse> getCheckAccessMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CheckAccess",
      requestType = alluxio.grpc.CheckAccessPRequest.class,
      responseType = alluxio.grpc.CheckAccessPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CheckAccessPRequest,
      alluxio.grpc.CheckAccessPResponse> getCheckAccessMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CheckAccessPRequest, alluxio.grpc.CheckAccessPResponse> getCheckAccessMethod;
    if ((getCheckAccessMethod = FileSystemMasterClientServiceGrpc.getCheckAccessMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getCheckAccessMethod = FileSystemMasterClientServiceGrpc.getCheckAccessMethod) == null) {
          FileSystemMasterClientServiceGrpc.getCheckAccessMethod = getCheckAccessMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.CheckAccessPRequest, alluxio.grpc.CheckAccessPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CheckAccess"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CheckAccessPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CheckAccessPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("CheckAccess"))
              .build();
        }
      }
    }
    return getCheckAccessMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CheckConsistencyPRequest,
      alluxio.grpc.CheckConsistencyPResponse> getCheckConsistencyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CheckConsistency",
      requestType = alluxio.grpc.CheckConsistencyPRequest.class,
      responseType = alluxio.grpc.CheckConsistencyPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CheckConsistencyPRequest,
      alluxio.grpc.CheckConsistencyPResponse> getCheckConsistencyMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CheckConsistencyPRequest, alluxio.grpc.CheckConsistencyPResponse> getCheckConsistencyMethod;
    if ((getCheckConsistencyMethod = FileSystemMasterClientServiceGrpc.getCheckConsistencyMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getCheckConsistencyMethod = FileSystemMasterClientServiceGrpc.getCheckConsistencyMethod) == null) {
          FileSystemMasterClientServiceGrpc.getCheckConsistencyMethod = getCheckConsistencyMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.CheckConsistencyPRequest, alluxio.grpc.CheckConsistencyPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CheckConsistency"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CheckConsistencyPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CheckConsistencyPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("CheckConsistency"))
              .build();
        }
      }
    }
    return getCheckConsistencyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.ExistsPRequest,
      alluxio.grpc.ExistsPResponse> getExistsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Exists",
      requestType = alluxio.grpc.ExistsPRequest.class,
      responseType = alluxio.grpc.ExistsPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.ExistsPRequest,
      alluxio.grpc.ExistsPResponse> getExistsMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.ExistsPRequest, alluxio.grpc.ExistsPResponse> getExistsMethod;
    if ((getExistsMethod = FileSystemMasterClientServiceGrpc.getExistsMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getExistsMethod = FileSystemMasterClientServiceGrpc.getExistsMethod) == null) {
          FileSystemMasterClientServiceGrpc.getExistsMethod = getExistsMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.ExistsPRequest, alluxio.grpc.ExistsPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Exists"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ExistsPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ExistsPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("Exists"))
              .build();
        }
      }
    }
    return getExistsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CompleteFilePRequest,
      alluxio.grpc.CompleteFilePResponse> getCompleteFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CompleteFile",
      requestType = alluxio.grpc.CompleteFilePRequest.class,
      responseType = alluxio.grpc.CompleteFilePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CompleteFilePRequest,
      alluxio.grpc.CompleteFilePResponse> getCompleteFileMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CompleteFilePRequest, alluxio.grpc.CompleteFilePResponse> getCompleteFileMethod;
    if ((getCompleteFileMethod = FileSystemMasterClientServiceGrpc.getCompleteFileMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getCompleteFileMethod = FileSystemMasterClientServiceGrpc.getCompleteFileMethod) == null) {
          FileSystemMasterClientServiceGrpc.getCompleteFileMethod = getCompleteFileMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.CompleteFilePRequest, alluxio.grpc.CompleteFilePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CompleteFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CompleteFilePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CompleteFilePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("CompleteFile"))
              .build();
        }
      }
    }
    return getCompleteFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CreateDirectoryPRequest,
      alluxio.grpc.CreateDirectoryPResponse> getCreateDirectoryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateDirectory",
      requestType = alluxio.grpc.CreateDirectoryPRequest.class,
      responseType = alluxio.grpc.CreateDirectoryPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CreateDirectoryPRequest,
      alluxio.grpc.CreateDirectoryPResponse> getCreateDirectoryMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CreateDirectoryPRequest, alluxio.grpc.CreateDirectoryPResponse> getCreateDirectoryMethod;
    if ((getCreateDirectoryMethod = FileSystemMasterClientServiceGrpc.getCreateDirectoryMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getCreateDirectoryMethod = FileSystemMasterClientServiceGrpc.getCreateDirectoryMethod) == null) {
          FileSystemMasterClientServiceGrpc.getCreateDirectoryMethod = getCreateDirectoryMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.CreateDirectoryPRequest, alluxio.grpc.CreateDirectoryPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateDirectory"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CreateDirectoryPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CreateDirectoryPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("CreateDirectory"))
              .build();
        }
      }
    }
    return getCreateDirectoryMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.CreateFilePRequest,
      alluxio.grpc.CreateFilePResponse> getCreateFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateFile",
      requestType = alluxio.grpc.CreateFilePRequest.class,
      responseType = alluxio.grpc.CreateFilePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.CreateFilePRequest,
      alluxio.grpc.CreateFilePResponse> getCreateFileMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.CreateFilePRequest, alluxio.grpc.CreateFilePResponse> getCreateFileMethod;
    if ((getCreateFileMethod = FileSystemMasterClientServiceGrpc.getCreateFileMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getCreateFileMethod = FileSystemMasterClientServiceGrpc.getCreateFileMethod) == null) {
          FileSystemMasterClientServiceGrpc.getCreateFileMethod = getCreateFileMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.CreateFilePRequest, alluxio.grpc.CreateFilePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CreateFilePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.CreateFilePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("CreateFile"))
              .build();
        }
      }
    }
    return getCreateFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.FreePRequest,
      alluxio.grpc.FreePResponse> getFreeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Free",
      requestType = alluxio.grpc.FreePRequest.class,
      responseType = alluxio.grpc.FreePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.FreePRequest,
      alluxio.grpc.FreePResponse> getFreeMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.FreePRequest, alluxio.grpc.FreePResponse> getFreeMethod;
    if ((getFreeMethod = FileSystemMasterClientServiceGrpc.getFreeMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getFreeMethod = FileSystemMasterClientServiceGrpc.getFreeMethod) == null) {
          FileSystemMasterClientServiceGrpc.getFreeMethod = getFreeMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.FreePRequest, alluxio.grpc.FreePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Free"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.FreePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.FreePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("Free"))
              .build();
        }
      }
    }
    return getFreeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.FreeWorkerPRequest,
      alluxio.grpc.FreeWorkerPResponse> getFreeWorkerMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "FreeWorker",
      requestType = alluxio.grpc.FreeWorkerPRequest.class,
      responseType = alluxio.grpc.FreeWorkerPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.FreeWorkerPRequest,
      alluxio.grpc.FreeWorkerPResponse> getFreeWorkerMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.FreeWorkerPRequest, alluxio.grpc.FreeWorkerPResponse> getFreeWorkerMethod;
    if ((getFreeWorkerMethod = FileSystemMasterClientServiceGrpc.getFreeWorkerMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getFreeWorkerMethod = FileSystemMasterClientServiceGrpc.getFreeWorkerMethod) == null) {
          FileSystemMasterClientServiceGrpc.getFreeWorkerMethod = getFreeWorkerMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.FreeWorkerPRequest, alluxio.grpc.FreeWorkerPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "FreeWorker"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.FreeWorkerPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.FreeWorkerPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("FreeWorker"))
              .build();
        }
      }
    }
    return getFreeWorkerMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetFilePathPRequest,
      alluxio.grpc.GetFilePathPResponse> getGetFilePathMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetFilePath",
      requestType = alluxio.grpc.GetFilePathPRequest.class,
      responseType = alluxio.grpc.GetFilePathPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetFilePathPRequest,
      alluxio.grpc.GetFilePathPResponse> getGetFilePathMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetFilePathPRequest, alluxio.grpc.GetFilePathPResponse> getGetFilePathMethod;
    if ((getGetFilePathMethod = FileSystemMasterClientServiceGrpc.getGetFilePathMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getGetFilePathMethod = FileSystemMasterClientServiceGrpc.getGetFilePathMethod) == null) {
          FileSystemMasterClientServiceGrpc.getGetFilePathMethod = getGetFilePathMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetFilePathPRequest, alluxio.grpc.GetFilePathPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetFilePath"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetFilePathPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetFilePathPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("GetFilePath"))
              .build();
        }
      }
    }
    return getGetFilePathMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetMountTablePRequest,
      alluxio.grpc.GetMountTablePResponse> getGetMountTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetMountTable",
      requestType = alluxio.grpc.GetMountTablePRequest.class,
      responseType = alluxio.grpc.GetMountTablePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetMountTablePRequest,
      alluxio.grpc.GetMountTablePResponse> getGetMountTableMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetMountTablePRequest, alluxio.grpc.GetMountTablePResponse> getGetMountTableMethod;
    if ((getGetMountTableMethod = FileSystemMasterClientServiceGrpc.getGetMountTableMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getGetMountTableMethod = FileSystemMasterClientServiceGrpc.getGetMountTableMethod) == null) {
          FileSystemMasterClientServiceGrpc.getGetMountTableMethod = getGetMountTableMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetMountTablePRequest, alluxio.grpc.GetMountTablePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetMountTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetMountTablePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetMountTablePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("GetMountTable"))
              .build();
        }
      }
    }
    return getGetMountTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetSyncPathListPRequest,
      alluxio.grpc.GetSyncPathListPResponse> getGetSyncPathListMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetSyncPathList",
      requestType = alluxio.grpc.GetSyncPathListPRequest.class,
      responseType = alluxio.grpc.GetSyncPathListPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetSyncPathListPRequest,
      alluxio.grpc.GetSyncPathListPResponse> getGetSyncPathListMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetSyncPathListPRequest, alluxio.grpc.GetSyncPathListPResponse> getGetSyncPathListMethod;
    if ((getGetSyncPathListMethod = FileSystemMasterClientServiceGrpc.getGetSyncPathListMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getGetSyncPathListMethod = FileSystemMasterClientServiceGrpc.getGetSyncPathListMethod) == null) {
          FileSystemMasterClientServiceGrpc.getGetSyncPathListMethod = getGetSyncPathListMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetSyncPathListPRequest, alluxio.grpc.GetSyncPathListPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetSyncPathList"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetSyncPathListPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetSyncPathListPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("GetSyncPathList"))
              .build();
        }
      }
    }
    return getGetSyncPathListMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetNewBlockIdForFilePRequest,
      alluxio.grpc.GetNewBlockIdForFilePResponse> getGetNewBlockIdForFileMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetNewBlockIdForFile",
      requestType = alluxio.grpc.GetNewBlockIdForFilePRequest.class,
      responseType = alluxio.grpc.GetNewBlockIdForFilePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetNewBlockIdForFilePRequest,
      alluxio.grpc.GetNewBlockIdForFilePResponse> getGetNewBlockIdForFileMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetNewBlockIdForFilePRequest, alluxio.grpc.GetNewBlockIdForFilePResponse> getGetNewBlockIdForFileMethod;
    if ((getGetNewBlockIdForFileMethod = FileSystemMasterClientServiceGrpc.getGetNewBlockIdForFileMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getGetNewBlockIdForFileMethod = FileSystemMasterClientServiceGrpc.getGetNewBlockIdForFileMethod) == null) {
          FileSystemMasterClientServiceGrpc.getGetNewBlockIdForFileMethod = getGetNewBlockIdForFileMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetNewBlockIdForFilePRequest, alluxio.grpc.GetNewBlockIdForFilePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetNewBlockIdForFile"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetNewBlockIdForFilePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetNewBlockIdForFilePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("GetNewBlockIdForFile"))
              .build();
        }
      }
    }
    return getGetNewBlockIdForFileMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetStatusPRequest,
      alluxio.grpc.GetStatusPResponse> getGetStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetStatus",
      requestType = alluxio.grpc.GetStatusPRequest.class,
      responseType = alluxio.grpc.GetStatusPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetStatusPRequest,
      alluxio.grpc.GetStatusPResponse> getGetStatusMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetStatusPRequest, alluxio.grpc.GetStatusPResponse> getGetStatusMethod;
    if ((getGetStatusMethod = FileSystemMasterClientServiceGrpc.getGetStatusMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getGetStatusMethod = FileSystemMasterClientServiceGrpc.getGetStatusMethod) == null) {
          FileSystemMasterClientServiceGrpc.getGetStatusMethod = getGetStatusMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetStatusPRequest, alluxio.grpc.GetStatusPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetStatusPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetStatusPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("GetStatus"))
              .build();
        }
      }
    }
    return getGetStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.ListStatusPRequest,
      alluxio.grpc.ListStatusPResponse> getListStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListStatus",
      requestType = alluxio.grpc.ListStatusPRequest.class,
      responseType = alluxio.grpc.ListStatusPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<alluxio.grpc.ListStatusPRequest,
      alluxio.grpc.ListStatusPResponse> getListStatusMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.ListStatusPRequest, alluxio.grpc.ListStatusPResponse> getListStatusMethod;
    if ((getListStatusMethod = FileSystemMasterClientServiceGrpc.getListStatusMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getListStatusMethod = FileSystemMasterClientServiceGrpc.getListStatusMethod) == null) {
          FileSystemMasterClientServiceGrpc.getListStatusMethod = getListStatusMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.ListStatusPRequest, alluxio.grpc.ListStatusPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ListStatusPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ListStatusPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("ListStatus"))
              .build();
        }
      }
    }
    return getListStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.MountPRequest,
      alluxio.grpc.MountPResponse> getMountMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Mount",
      requestType = alluxio.grpc.MountPRequest.class,
      responseType = alluxio.grpc.MountPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.MountPRequest,
      alluxio.grpc.MountPResponse> getMountMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.MountPRequest, alluxio.grpc.MountPResponse> getMountMethod;
    if ((getMountMethod = FileSystemMasterClientServiceGrpc.getMountMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getMountMethod = FileSystemMasterClientServiceGrpc.getMountMethod) == null) {
          FileSystemMasterClientServiceGrpc.getMountMethod = getMountMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.MountPRequest, alluxio.grpc.MountPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Mount"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.MountPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.MountPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("Mount"))
              .build();
        }
      }
    }
    return getMountMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.DeletePRequest,
      alluxio.grpc.DeletePResponse> getRemoveMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Remove",
      requestType = alluxio.grpc.DeletePRequest.class,
      responseType = alluxio.grpc.DeletePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.DeletePRequest,
      alluxio.grpc.DeletePResponse> getRemoveMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.DeletePRequest, alluxio.grpc.DeletePResponse> getRemoveMethod;
    if ((getRemoveMethod = FileSystemMasterClientServiceGrpc.getRemoveMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getRemoveMethod = FileSystemMasterClientServiceGrpc.getRemoveMethod) == null) {
          FileSystemMasterClientServiceGrpc.getRemoveMethod = getRemoveMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.DeletePRequest, alluxio.grpc.DeletePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Remove"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.DeletePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.DeletePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("Remove"))
              .build();
        }
      }
    }
    return getRemoveMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.RenamePRequest,
      alluxio.grpc.RenamePResponse> getRenameMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Rename",
      requestType = alluxio.grpc.RenamePRequest.class,
      responseType = alluxio.grpc.RenamePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.RenamePRequest,
      alluxio.grpc.RenamePResponse> getRenameMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.RenamePRequest, alluxio.grpc.RenamePResponse> getRenameMethod;
    if ((getRenameMethod = FileSystemMasterClientServiceGrpc.getRenameMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getRenameMethod = FileSystemMasterClientServiceGrpc.getRenameMethod) == null) {
          FileSystemMasterClientServiceGrpc.getRenameMethod = getRenameMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.RenamePRequest, alluxio.grpc.RenamePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Rename"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RenamePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.RenamePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("Rename"))
              .build();
        }
      }
    }
    return getRenameMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.ReverseResolvePRequest,
      alluxio.grpc.ReverseResolvePResponse> getReverseResolveMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ReverseResolve",
      requestType = alluxio.grpc.ReverseResolvePRequest.class,
      responseType = alluxio.grpc.ReverseResolvePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.ReverseResolvePRequest,
      alluxio.grpc.ReverseResolvePResponse> getReverseResolveMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.ReverseResolvePRequest, alluxio.grpc.ReverseResolvePResponse> getReverseResolveMethod;
    if ((getReverseResolveMethod = FileSystemMasterClientServiceGrpc.getReverseResolveMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getReverseResolveMethod = FileSystemMasterClientServiceGrpc.getReverseResolveMethod) == null) {
          FileSystemMasterClientServiceGrpc.getReverseResolveMethod = getReverseResolveMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.ReverseResolvePRequest, alluxio.grpc.ReverseResolvePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ReverseResolve"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ReverseResolvePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ReverseResolvePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("ReverseResolve"))
              .build();
        }
      }
    }
    return getReverseResolveMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.ScheduleAsyncPersistencePRequest,
      alluxio.grpc.ScheduleAsyncPersistencePResponse> getScheduleAsyncPersistenceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScheduleAsyncPersistence",
      requestType = alluxio.grpc.ScheduleAsyncPersistencePRequest.class,
      responseType = alluxio.grpc.ScheduleAsyncPersistencePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.ScheduleAsyncPersistencePRequest,
      alluxio.grpc.ScheduleAsyncPersistencePResponse> getScheduleAsyncPersistenceMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.ScheduleAsyncPersistencePRequest, alluxio.grpc.ScheduleAsyncPersistencePResponse> getScheduleAsyncPersistenceMethod;
    if ((getScheduleAsyncPersistenceMethod = FileSystemMasterClientServiceGrpc.getScheduleAsyncPersistenceMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getScheduleAsyncPersistenceMethod = FileSystemMasterClientServiceGrpc.getScheduleAsyncPersistenceMethod) == null) {
          FileSystemMasterClientServiceGrpc.getScheduleAsyncPersistenceMethod = getScheduleAsyncPersistenceMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.ScheduleAsyncPersistencePRequest, alluxio.grpc.ScheduleAsyncPersistencePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScheduleAsyncPersistence"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ScheduleAsyncPersistencePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.ScheduleAsyncPersistencePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("ScheduleAsyncPersistence"))
              .build();
        }
      }
    }
    return getScheduleAsyncPersistenceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.SetAclPRequest,
      alluxio.grpc.SetAclPResponse> getSetAclMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetAcl",
      requestType = alluxio.grpc.SetAclPRequest.class,
      responseType = alluxio.grpc.SetAclPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.SetAclPRequest,
      alluxio.grpc.SetAclPResponse> getSetAclMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.SetAclPRequest, alluxio.grpc.SetAclPResponse> getSetAclMethod;
    if ((getSetAclMethod = FileSystemMasterClientServiceGrpc.getSetAclMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getSetAclMethod = FileSystemMasterClientServiceGrpc.getSetAclMethod) == null) {
          FileSystemMasterClientServiceGrpc.getSetAclMethod = getSetAclMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.SetAclPRequest, alluxio.grpc.SetAclPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetAcl"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.SetAclPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.SetAclPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("SetAcl"))
              .build();
        }
      }
    }
    return getSetAclMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.SetAttributePRequest,
      alluxio.grpc.SetAttributePResponse> getSetAttributeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SetAttribute",
      requestType = alluxio.grpc.SetAttributePRequest.class,
      responseType = alluxio.grpc.SetAttributePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.SetAttributePRequest,
      alluxio.grpc.SetAttributePResponse> getSetAttributeMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.SetAttributePRequest, alluxio.grpc.SetAttributePResponse> getSetAttributeMethod;
    if ((getSetAttributeMethod = FileSystemMasterClientServiceGrpc.getSetAttributeMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getSetAttributeMethod = FileSystemMasterClientServiceGrpc.getSetAttributeMethod) == null) {
          FileSystemMasterClientServiceGrpc.getSetAttributeMethod = getSetAttributeMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.SetAttributePRequest, alluxio.grpc.SetAttributePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SetAttribute"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.SetAttributePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.SetAttributePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("SetAttribute"))
              .build();
        }
      }
    }
    return getSetAttributeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.StartSyncPRequest,
      alluxio.grpc.StartSyncPResponse> getStartSyncMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StartSync",
      requestType = alluxio.grpc.StartSyncPRequest.class,
      responseType = alluxio.grpc.StartSyncPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.StartSyncPRequest,
      alluxio.grpc.StartSyncPResponse> getStartSyncMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.StartSyncPRequest, alluxio.grpc.StartSyncPResponse> getStartSyncMethod;
    if ((getStartSyncMethod = FileSystemMasterClientServiceGrpc.getStartSyncMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getStartSyncMethod = FileSystemMasterClientServiceGrpc.getStartSyncMethod) == null) {
          FileSystemMasterClientServiceGrpc.getStartSyncMethod = getStartSyncMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.StartSyncPRequest, alluxio.grpc.StartSyncPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StartSync"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.StartSyncPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.StartSyncPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("StartSync"))
              .build();
        }
      }
    }
    return getStartSyncMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.StopSyncPRequest,
      alluxio.grpc.StopSyncPResponse> getStopSyncMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StopSync",
      requestType = alluxio.grpc.StopSyncPRequest.class,
      responseType = alluxio.grpc.StopSyncPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.StopSyncPRequest,
      alluxio.grpc.StopSyncPResponse> getStopSyncMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.StopSyncPRequest, alluxio.grpc.StopSyncPResponse> getStopSyncMethod;
    if ((getStopSyncMethod = FileSystemMasterClientServiceGrpc.getStopSyncMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getStopSyncMethod = FileSystemMasterClientServiceGrpc.getStopSyncMethod) == null) {
          FileSystemMasterClientServiceGrpc.getStopSyncMethod = getStopSyncMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.StopSyncPRequest, alluxio.grpc.StopSyncPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StopSync"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.StopSyncPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.StopSyncPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("StopSync"))
              .build();
        }
      }
    }
    return getStopSyncMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.UnmountPRequest,
      alluxio.grpc.UnmountPResponse> getUnmountMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Unmount",
      requestType = alluxio.grpc.UnmountPRequest.class,
      responseType = alluxio.grpc.UnmountPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.UnmountPRequest,
      alluxio.grpc.UnmountPResponse> getUnmountMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.UnmountPRequest, alluxio.grpc.UnmountPResponse> getUnmountMethod;
    if ((getUnmountMethod = FileSystemMasterClientServiceGrpc.getUnmountMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getUnmountMethod = FileSystemMasterClientServiceGrpc.getUnmountMethod) == null) {
          FileSystemMasterClientServiceGrpc.getUnmountMethod = getUnmountMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.UnmountPRequest, alluxio.grpc.UnmountPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Unmount"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.UnmountPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.UnmountPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("Unmount"))
              .build();
        }
      }
    }
    return getUnmountMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.UpdateMountPRequest,
      alluxio.grpc.UpdateMountPResponse> getUpdateMountMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateMount",
      requestType = alluxio.grpc.UpdateMountPRequest.class,
      responseType = alluxio.grpc.UpdateMountPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.UpdateMountPRequest,
      alluxio.grpc.UpdateMountPResponse> getUpdateMountMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.UpdateMountPRequest, alluxio.grpc.UpdateMountPResponse> getUpdateMountMethod;
    if ((getUpdateMountMethod = FileSystemMasterClientServiceGrpc.getUpdateMountMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getUpdateMountMethod = FileSystemMasterClientServiceGrpc.getUpdateMountMethod) == null) {
          FileSystemMasterClientServiceGrpc.getUpdateMountMethod = getUpdateMountMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.UpdateMountPRequest, alluxio.grpc.UpdateMountPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateMount"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.UpdateMountPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.UpdateMountPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("UpdateMount"))
              .build();
        }
      }
    }
    return getUpdateMountMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.UpdateUfsModePRequest,
      alluxio.grpc.UpdateUfsModePResponse> getUpdateUfsModeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateUfsMode",
      requestType = alluxio.grpc.UpdateUfsModePRequest.class,
      responseType = alluxio.grpc.UpdateUfsModePResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.UpdateUfsModePRequest,
      alluxio.grpc.UpdateUfsModePResponse> getUpdateUfsModeMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.UpdateUfsModePRequest, alluxio.grpc.UpdateUfsModePResponse> getUpdateUfsModeMethod;
    if ((getUpdateUfsModeMethod = FileSystemMasterClientServiceGrpc.getUpdateUfsModeMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getUpdateUfsModeMethod = FileSystemMasterClientServiceGrpc.getUpdateUfsModeMethod) == null) {
          FileSystemMasterClientServiceGrpc.getUpdateUfsModeMethod = getUpdateUfsModeMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.UpdateUfsModePRequest, alluxio.grpc.UpdateUfsModePResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateUfsMode"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.UpdateUfsModePRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.UpdateUfsModePResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("UpdateUfsMode"))
              .build();
        }
      }
    }
    return getUpdateUfsModeMethod;
  }

  private static volatile io.grpc.MethodDescriptor<alluxio.grpc.GetStateLockHoldersPRequest,
      alluxio.grpc.GetStateLockHoldersPResponse> getGetStateLockHoldersMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetStateLockHolders",
      requestType = alluxio.grpc.GetStateLockHoldersPRequest.class,
      responseType = alluxio.grpc.GetStateLockHoldersPResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.GetStateLockHoldersPRequest,
      alluxio.grpc.GetStateLockHoldersPResponse> getGetStateLockHoldersMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.GetStateLockHoldersPRequest, alluxio.grpc.GetStateLockHoldersPResponse> getGetStateLockHoldersMethod;
    if ((getGetStateLockHoldersMethod = FileSystemMasterClientServiceGrpc.getGetStateLockHoldersMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getGetStateLockHoldersMethod = FileSystemMasterClientServiceGrpc.getGetStateLockHoldersMethod) == null) {
          FileSystemMasterClientServiceGrpc.getGetStateLockHoldersMethod = getGetStateLockHoldersMethod =
              io.grpc.MethodDescriptor.<alluxio.grpc.GetStateLockHoldersPRequest, alluxio.grpc.GetStateLockHoldersPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetStateLockHolders"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetStateLockHoldersPRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  alluxio.grpc.GetStateLockHoldersPResponse.getDefaultInstance()))
              .setSchemaDescriptor(new FileSystemMasterClientServiceMethodDescriptorSupplier("GetStateLockHolders"))
              .build();
        }
      }
    }
    return getGetStateLockHoldersMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static FileSystemMasterClientServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<FileSystemMasterClientServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<FileSystemMasterClientServiceStub>() {
        @java.lang.Override
        public FileSystemMasterClientServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new FileSystemMasterClientServiceStub(channel, callOptions);
        }
      };
    return FileSystemMasterClientServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static FileSystemMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<FileSystemMasterClientServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<FileSystemMasterClientServiceBlockingStub>() {
        @java.lang.Override
        public FileSystemMasterClientServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new FileSystemMasterClientServiceBlockingStub(channel, callOptions);
        }
      };
    return FileSystemMasterClientServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static FileSystemMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<FileSystemMasterClientServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<FileSystemMasterClientServiceFutureStub>() {
        @java.lang.Override
        public FileSystemMasterClientServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new FileSystemMasterClientServiceFutureStub(channel, callOptions);
        }
      };
    return FileSystemMasterClientServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   **
   * This interface contains file system master service endpoints for Alluxio clients.
   * </pre>
   */
  public static abstract class FileSystemMasterClientServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     **
     * Checks access to path.
     * </pre>
     */
    public void checkAccess(alluxio.grpc.CheckAccessPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CheckAccessPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCheckAccessMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Checks the consistency of the files and directores with the path as the root of the subtree
     * </pre>
     */
    public void checkConsistency(alluxio.grpc.CheckConsistencyPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CheckConsistencyPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCheckConsistencyMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Checks the existence of the file or directory.
     * </pre>
     */
    public void exists(alluxio.grpc.ExistsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ExistsPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExistsMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Marks a file as completed.
     * </pre>
     */
    public void completeFile(alluxio.grpc.CompleteFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CompleteFilePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCompleteFileMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Creates a directory.
     * </pre>
     */
    public void createDirectory(alluxio.grpc.CreateDirectoryPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateDirectoryPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateDirectoryMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Creates a file.
     * </pre>
     */
    public void createFile(alluxio.grpc.CreateFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateFilePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateFileMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Frees the given file or directory from Alluxio.
     * </pre>
     */
    public void free(alluxio.grpc.FreePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.FreePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFreeMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Frees the given Worker from Alluxio.
     * </pre>
     */
    public void freeWorker(alluxio.grpc.FreeWorkerPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.FreeWorkerPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFreeWorkerMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the file path of a file id
     * </pre>
     */
    public void getFilePath(alluxio.grpc.GetFilePathPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetFilePathPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetFilePathMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a map from each Alluxio path to information of corresponding mount point
     * </pre>
     */
    public void getMountTable(alluxio.grpc.GetMountTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetMountTablePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetMountTableMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a list of paths that are being actively synced by Alluxio
     * </pre>
     */
    public void getSyncPathList(alluxio.grpc.GetSyncPathListPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetSyncPathListPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetSyncPathListMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Generates a new block id for the given file.
     * </pre>
     */
    public void getNewBlockIdForFile(alluxio.grpc.GetNewBlockIdForFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetNewBlockIdForFilePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetNewBlockIdForFileMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the status of the file or directory.
     * </pre>
     */
    public void getStatus(alluxio.grpc.GetStatusPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetStatusPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetStatusMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * If the path points to a file, the method returns a singleton with its file information.
     * If the path points to a directory, the method returns a list with file information for the
     * directory contents.
     * </pre>
     */
    public void listStatus(alluxio.grpc.ListStatusPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ListStatusPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListStatusMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Creates a new "mount point", mounts the given UFS path in the Alluxio namespace at the given
     * path. The path should not exist and should not be nested under any existing mount point.
     * </pre>
     */
    public void mount(alluxio.grpc.MountPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.MountPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMountMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Deletes a file or a directory and returns whether the remove operation succeeded.
     * </pre>
     */
    public void remove(alluxio.grpc.DeletePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.DeletePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRemoveMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Renames a file or a directory.
     * </pre>
     */
    public void rename(alluxio.grpc.RenamePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RenamePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRenameMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Reverse resolve a ufs path.
     * </pre>
     */
    public void reverseResolve(alluxio.grpc.ReverseResolvePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ReverseResolvePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReverseResolveMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Schedules async persistence.
     * </pre>
     */
    public void scheduleAsyncPersistence(alluxio.grpc.ScheduleAsyncPersistencePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ScheduleAsyncPersistencePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScheduleAsyncPersistenceMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Sets ACL for the path.
     * </pre>
     */
    public void setAcl(alluxio.grpc.SetAclPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.SetAclPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetAclMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Sets file or directory attributes.
     * </pre>
     */
    public void setAttribute(alluxio.grpc.SetAttributePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.SetAttributePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSetAttributeMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Start the active syncing of the directory or file
     * </pre>
     */
    public void startSync(alluxio.grpc.StartSyncPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.StartSyncPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStartSyncMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Start the active syncing of the directory or file
     * </pre>
     */
    public void stopSync(alluxio.grpc.StopSyncPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.StopSyncPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStopSyncMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Deletes an existing "mount point", voiding the Alluxio namespace at the given path. The path
     * should correspond to an existing mount point. Any files in its subtree that are backed by UFS
     * will be persisted before they are removed from the Alluxio namespace.
     * </pre>
     */
    public void unmount(alluxio.grpc.UnmountPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.UnmountPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUnmountMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Updates an existing "mount point", changing its mount properties
     * </pre>
     */
    public void updateMount(alluxio.grpc.UpdateMountPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.UpdateMountPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateMountMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Updates the ufs mode for a ufs path under one or more mount points.
     * </pre>
     */
    public void updateUfsMode(alluxio.grpc.UpdateUfsModePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.UpdateUfsModePResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateUfsModeMethod(), responseObserver);
    }

    /**
     */
    public void getStateLockHolders(alluxio.grpc.GetStateLockHoldersPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetStateLockHoldersPResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetStateLockHoldersMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCheckAccessMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CheckAccessPRequest,
                alluxio.grpc.CheckAccessPResponse>(
                  this, METHODID_CHECK_ACCESS)))
          .addMethod(
            getCheckConsistencyMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CheckConsistencyPRequest,
                alluxio.grpc.CheckConsistencyPResponse>(
                  this, METHODID_CHECK_CONSISTENCY)))
          .addMethod(
            getExistsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.ExistsPRequest,
                alluxio.grpc.ExistsPResponse>(
                  this, METHODID_EXISTS)))
          .addMethod(
            getCompleteFileMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CompleteFilePRequest,
                alluxio.grpc.CompleteFilePResponse>(
                  this, METHODID_COMPLETE_FILE)))
          .addMethod(
            getCreateDirectoryMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CreateDirectoryPRequest,
                alluxio.grpc.CreateDirectoryPResponse>(
                  this, METHODID_CREATE_DIRECTORY)))
          .addMethod(
            getCreateFileMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CreateFilePRequest,
                alluxio.grpc.CreateFilePResponse>(
                  this, METHODID_CREATE_FILE)))
          .addMethod(
            getFreeMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.FreePRequest,
                alluxio.grpc.FreePResponse>(
                  this, METHODID_FREE)))
          .addMethod(
            getFreeWorkerMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.FreeWorkerPRequest,
                alluxio.grpc.FreeWorkerPResponse>(
                  this, METHODID_FREE_WORKER)))
          .addMethod(
            getGetFilePathMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetFilePathPRequest,
                alluxio.grpc.GetFilePathPResponse>(
                  this, METHODID_GET_FILE_PATH)))
          .addMethod(
            getGetMountTableMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetMountTablePRequest,
                alluxio.grpc.GetMountTablePResponse>(
                  this, METHODID_GET_MOUNT_TABLE)))
          .addMethod(
            getGetSyncPathListMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetSyncPathListPRequest,
                alluxio.grpc.GetSyncPathListPResponse>(
                  this, METHODID_GET_SYNC_PATH_LIST)))
          .addMethod(
            getGetNewBlockIdForFileMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetNewBlockIdForFilePRequest,
                alluxio.grpc.GetNewBlockIdForFilePResponse>(
                  this, METHODID_GET_NEW_BLOCK_ID_FOR_FILE)))
          .addMethod(
            getGetStatusMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetStatusPRequest,
                alluxio.grpc.GetStatusPResponse>(
                  this, METHODID_GET_STATUS)))
          .addMethod(
            getListStatusMethod(),
            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
              new MethodHandlers<
                alluxio.grpc.ListStatusPRequest,
                alluxio.grpc.ListStatusPResponse>(
                  this, METHODID_LIST_STATUS)))
          .addMethod(
            getMountMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.MountPRequest,
                alluxio.grpc.MountPResponse>(
                  this, METHODID_MOUNT)))
          .addMethod(
            getRemoveMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.DeletePRequest,
                alluxio.grpc.DeletePResponse>(
                  this, METHODID_REMOVE)))
          .addMethod(
            getRenameMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RenamePRequest,
                alluxio.grpc.RenamePResponse>(
                  this, METHODID_RENAME)))
          .addMethod(
            getReverseResolveMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.ReverseResolvePRequest,
                alluxio.grpc.ReverseResolvePResponse>(
                  this, METHODID_REVERSE_RESOLVE)))
          .addMethod(
            getScheduleAsyncPersistenceMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.ScheduleAsyncPersistencePRequest,
                alluxio.grpc.ScheduleAsyncPersistencePResponse>(
                  this, METHODID_SCHEDULE_ASYNC_PERSISTENCE)))
          .addMethod(
            getSetAclMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.SetAclPRequest,
                alluxio.grpc.SetAclPResponse>(
                  this, METHODID_SET_ACL)))
          .addMethod(
            getSetAttributeMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.SetAttributePRequest,
                alluxio.grpc.SetAttributePResponse>(
                  this, METHODID_SET_ATTRIBUTE)))
          .addMethod(
            getStartSyncMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.StartSyncPRequest,
                alluxio.grpc.StartSyncPResponse>(
                  this, METHODID_START_SYNC)))
          .addMethod(
            getStopSyncMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.StopSyncPRequest,
                alluxio.grpc.StopSyncPResponse>(
                  this, METHODID_STOP_SYNC)))
          .addMethod(
            getUnmountMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.UnmountPRequest,
                alluxio.grpc.UnmountPResponse>(
                  this, METHODID_UNMOUNT)))
          .addMethod(
            getUpdateMountMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.UpdateMountPRequest,
                alluxio.grpc.UpdateMountPResponse>(
                  this, METHODID_UPDATE_MOUNT)))
          .addMethod(
            getUpdateUfsModeMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.UpdateUfsModePRequest,
                alluxio.grpc.UpdateUfsModePResponse>(
                  this, METHODID_UPDATE_UFS_MODE)))
          .addMethod(
            getGetStateLockHoldersMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetStateLockHoldersPRequest,
                alluxio.grpc.GetStateLockHoldersPResponse>(
                  this, METHODID_GET_STATE_LOCK_HOLDERS)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains file system master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class FileSystemMasterClientServiceStub extends io.grpc.stub.AbstractAsyncStub<FileSystemMasterClientServiceStub> {
    private FileSystemMasterClientServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemMasterClientServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new FileSystemMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Checks access to path.
     * </pre>
     */
    public void checkAccess(alluxio.grpc.CheckAccessPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CheckAccessPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCheckAccessMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Checks the consistency of the files and directores with the path as the root of the subtree
     * </pre>
     */
    public void checkConsistency(alluxio.grpc.CheckConsistencyPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CheckConsistencyPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCheckConsistencyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Checks the existence of the file or directory.
     * </pre>
     */
    public void exists(alluxio.grpc.ExistsPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ExistsPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExistsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Marks a file as completed.
     * </pre>
     */
    public void completeFile(alluxio.grpc.CompleteFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CompleteFilePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCompleteFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Creates a directory.
     * </pre>
     */
    public void createDirectory(alluxio.grpc.CreateDirectoryPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateDirectoryPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateDirectoryMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Creates a file.
     * </pre>
     */
    public void createFile(alluxio.grpc.CreateFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateFilePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Frees the given file or directory from Alluxio.
     * </pre>
     */
    public void free(alluxio.grpc.FreePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.FreePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getFreeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Frees the given Worker from Alluxio.
     * </pre>
     */
    public void freeWorker(alluxio.grpc.FreeWorkerPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.FreeWorkerPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getFreeWorkerMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the file path of a file id
     * </pre>
     */
    public void getFilePath(alluxio.grpc.GetFilePathPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetFilePathPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetFilePathMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a map from each Alluxio path to information of corresponding mount point
     * </pre>
     */
    public void getMountTable(alluxio.grpc.GetMountTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetMountTablePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetMountTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a list of paths that are being actively synced by Alluxio
     * </pre>
     */
    public void getSyncPathList(alluxio.grpc.GetSyncPathListPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetSyncPathListPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetSyncPathListMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Generates a new block id for the given file.
     * </pre>
     */
    public void getNewBlockIdForFile(alluxio.grpc.GetNewBlockIdForFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetNewBlockIdForFilePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetNewBlockIdForFileMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the status of the file or directory.
     * </pre>
     */
    public void getStatus(alluxio.grpc.GetStatusPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetStatusPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * If the path points to a file, the method returns a singleton with its file information.
     * If the path points to a directory, the method returns a list with file information for the
     * directory contents.
     * </pre>
     */
    public void listStatus(alluxio.grpc.ListStatusPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ListStatusPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getListStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Creates a new "mount point", mounts the given UFS path in the Alluxio namespace at the given
     * path. The path should not exist and should not be nested under any existing mount point.
     * </pre>
     */
    public void mount(alluxio.grpc.MountPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.MountPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getMountMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Deletes a file or a directory and returns whether the remove operation succeeded.
     * </pre>
     */
    public void remove(alluxio.grpc.DeletePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.DeletePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRemoveMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Renames a file or a directory.
     * </pre>
     */
    public void rename(alluxio.grpc.RenamePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RenamePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getRenameMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Reverse resolve a ufs path.
     * </pre>
     */
    public void reverseResolve(alluxio.grpc.ReverseResolvePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ReverseResolvePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReverseResolveMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Schedules async persistence.
     * </pre>
     */
    public void scheduleAsyncPersistence(alluxio.grpc.ScheduleAsyncPersistencePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ScheduleAsyncPersistencePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScheduleAsyncPersistenceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Sets ACL for the path.
     * </pre>
     */
    public void setAcl(alluxio.grpc.SetAclPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.SetAclPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetAclMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Sets file or directory attributes.
     * </pre>
     */
    public void setAttribute(alluxio.grpc.SetAttributePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.SetAttributePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSetAttributeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Start the active syncing of the directory or file
     * </pre>
     */
    public void startSync(alluxio.grpc.StartSyncPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.StartSyncPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStartSyncMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Start the active syncing of the directory or file
     * </pre>
     */
    public void stopSync(alluxio.grpc.StopSyncPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.StopSyncPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getStopSyncMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Deletes an existing "mount point", voiding the Alluxio namespace at the given path. The path
     * should correspond to an existing mount point. Any files in its subtree that are backed by UFS
     * will be persisted before they are removed from the Alluxio namespace.
     * </pre>
     */
    public void unmount(alluxio.grpc.UnmountPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.UnmountPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUnmountMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Updates an existing "mount point", changing its mount properties
     * </pre>
     */
    public void updateMount(alluxio.grpc.UpdateMountPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.UpdateMountPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateMountMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Updates the ufs mode for a ufs path under one or more mount points.
     * </pre>
     */
    public void updateUfsMode(alluxio.grpc.UpdateUfsModePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.UpdateUfsModePResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateUfsModeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getStateLockHolders(alluxio.grpc.GetStateLockHoldersPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetStateLockHoldersPResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetStateLockHoldersMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains file system master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class FileSystemMasterClientServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<FileSystemMasterClientServiceBlockingStub> {
    private FileSystemMasterClientServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemMasterClientServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new FileSystemMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Checks access to path.
     * </pre>
     */
    public alluxio.grpc.CheckAccessPResponse checkAccess(alluxio.grpc.CheckAccessPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCheckAccessMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Checks the consistency of the files and directores with the path as the root of the subtree
     * </pre>
     */
    public alluxio.grpc.CheckConsistencyPResponse checkConsistency(alluxio.grpc.CheckConsistencyPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCheckConsistencyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Checks the existence of the file or directory.
     * </pre>
     */
    public alluxio.grpc.ExistsPResponse exists(alluxio.grpc.ExistsPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExistsMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Marks a file as completed.
     * </pre>
     */
    public alluxio.grpc.CompleteFilePResponse completeFile(alluxio.grpc.CompleteFilePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCompleteFileMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Creates a directory.
     * </pre>
     */
    public alluxio.grpc.CreateDirectoryPResponse createDirectory(alluxio.grpc.CreateDirectoryPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateDirectoryMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Creates a file.
     * </pre>
     */
    public alluxio.grpc.CreateFilePResponse createFile(alluxio.grpc.CreateFilePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateFileMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Frees the given file or directory from Alluxio.
     * </pre>
     */
    public alluxio.grpc.FreePResponse free(alluxio.grpc.FreePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getFreeMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Frees the given Worker from Alluxio.
     * </pre>
     */
    public alluxio.grpc.FreeWorkerPResponse freeWorker(alluxio.grpc.FreeWorkerPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getFreeWorkerMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns the file path of a file id
     * </pre>
     */
    public alluxio.grpc.GetFilePathPResponse getFilePath(alluxio.grpc.GetFilePathPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetFilePathMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns a map from each Alluxio path to information of corresponding mount point
     * </pre>
     */
    public alluxio.grpc.GetMountTablePResponse getMountTable(alluxio.grpc.GetMountTablePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetMountTableMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns a list of paths that are being actively synced by Alluxio
     * </pre>
     */
    public alluxio.grpc.GetSyncPathListPResponse getSyncPathList(alluxio.grpc.GetSyncPathListPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetSyncPathListMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Generates a new block id for the given file.
     * </pre>
     */
    public alluxio.grpc.GetNewBlockIdForFilePResponse getNewBlockIdForFile(alluxio.grpc.GetNewBlockIdForFilePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetNewBlockIdForFileMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns the status of the file or directory.
     * </pre>
     */
    public alluxio.grpc.GetStatusPResponse getStatus(alluxio.grpc.GetStatusPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetStatusMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * If the path points to a file, the method returns a singleton with its file information.
     * If the path points to a directory, the method returns a list with file information for the
     * directory contents.
     * </pre>
     */
    public java.util.Iterator<alluxio.grpc.ListStatusPResponse> listStatus(
        alluxio.grpc.ListStatusPRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getListStatusMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Creates a new "mount point", mounts the given UFS path in the Alluxio namespace at the given
     * path. The path should not exist and should not be nested under any existing mount point.
     * </pre>
     */
    public alluxio.grpc.MountPResponse mount(alluxio.grpc.MountPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getMountMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Deletes a file or a directory and returns whether the remove operation succeeded.
     * </pre>
     */
    public alluxio.grpc.DeletePResponse remove(alluxio.grpc.DeletePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRemoveMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Renames a file or a directory.
     * </pre>
     */
    public alluxio.grpc.RenamePResponse rename(alluxio.grpc.RenamePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getRenameMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Reverse resolve a ufs path.
     * </pre>
     */
    public alluxio.grpc.ReverseResolvePResponse reverseResolve(alluxio.grpc.ReverseResolvePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReverseResolveMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Schedules async persistence.
     * </pre>
     */
    public alluxio.grpc.ScheduleAsyncPersistencePResponse scheduleAsyncPersistence(alluxio.grpc.ScheduleAsyncPersistencePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScheduleAsyncPersistenceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Sets ACL for the path.
     * </pre>
     */
    public alluxio.grpc.SetAclPResponse setAcl(alluxio.grpc.SetAclPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetAclMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Sets file or directory attributes.
     * </pre>
     */
    public alluxio.grpc.SetAttributePResponse setAttribute(alluxio.grpc.SetAttributePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSetAttributeMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Start the active syncing of the directory or file
     * </pre>
     */
    public alluxio.grpc.StartSyncPResponse startSync(alluxio.grpc.StartSyncPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStartSyncMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Start the active syncing of the directory or file
     * </pre>
     */
    public alluxio.grpc.StopSyncPResponse stopSync(alluxio.grpc.StopSyncPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getStopSyncMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Deletes an existing "mount point", voiding the Alluxio namespace at the given path. The path
     * should correspond to an existing mount point. Any files in its subtree that are backed by UFS
     * will be persisted before they are removed from the Alluxio namespace.
     * </pre>
     */
    public alluxio.grpc.UnmountPResponse unmount(alluxio.grpc.UnmountPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUnmountMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Updates an existing "mount point", changing its mount properties
     * </pre>
     */
    public alluxio.grpc.UpdateMountPResponse updateMount(alluxio.grpc.UpdateMountPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateMountMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Updates the ufs mode for a ufs path under one or more mount points.
     * </pre>
     */
    public alluxio.grpc.UpdateUfsModePResponse updateUfsMode(alluxio.grpc.UpdateUfsModePRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateUfsModeMethod(), getCallOptions(), request);
    }

    /**
     */
    public alluxio.grpc.GetStateLockHoldersPResponse getStateLockHolders(alluxio.grpc.GetStateLockHoldersPRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetStateLockHoldersMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains file system master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class FileSystemMasterClientServiceFutureStub extends io.grpc.stub.AbstractFutureStub<FileSystemMasterClientServiceFutureStub> {
    private FileSystemMasterClientServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemMasterClientServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new FileSystemMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Checks access to path.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CheckAccessPResponse> checkAccess(
        alluxio.grpc.CheckAccessPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCheckAccessMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Checks the consistency of the files and directores with the path as the root of the subtree
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CheckConsistencyPResponse> checkConsistency(
        alluxio.grpc.CheckConsistencyPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCheckConsistencyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Checks the existence of the file or directory.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.ExistsPResponse> exists(
        alluxio.grpc.ExistsPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExistsMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Marks a file as completed.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CompleteFilePResponse> completeFile(
        alluxio.grpc.CompleteFilePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCompleteFileMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Creates a directory.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CreateDirectoryPResponse> createDirectory(
        alluxio.grpc.CreateDirectoryPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateDirectoryMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Creates a file.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CreateFilePResponse> createFile(
        alluxio.grpc.CreateFilePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateFileMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Frees the given file or directory from Alluxio.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.FreePResponse> free(
        alluxio.grpc.FreePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getFreeMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Frees the given Worker from Alluxio.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.FreeWorkerPResponse> freeWorker(
        alluxio.grpc.FreeWorkerPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getFreeWorkerMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns the file path of a file id
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetFilePathPResponse> getFilePath(
        alluxio.grpc.GetFilePathPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetFilePathMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns a map from each Alluxio path to information of corresponding mount point
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetMountTablePResponse> getMountTable(
        alluxio.grpc.GetMountTablePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetMountTableMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns a list of paths that are being actively synced by Alluxio
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetSyncPathListPResponse> getSyncPathList(
        alluxio.grpc.GetSyncPathListPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetSyncPathListMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Generates a new block id for the given file.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetNewBlockIdForFilePResponse> getNewBlockIdForFile(
        alluxio.grpc.GetNewBlockIdForFilePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetNewBlockIdForFileMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns the status of the file or directory.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetStatusPResponse> getStatus(
        alluxio.grpc.GetStatusPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetStatusMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Creates a new "mount point", mounts the given UFS path in the Alluxio namespace at the given
     * path. The path should not exist and should not be nested under any existing mount point.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.MountPResponse> mount(
        alluxio.grpc.MountPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getMountMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Deletes a file or a directory and returns whether the remove operation succeeded.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.DeletePResponse> remove(
        alluxio.grpc.DeletePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRemoveMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Renames a file or a directory.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.RenamePResponse> rename(
        alluxio.grpc.RenamePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getRenameMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Reverse resolve a ufs path.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.ReverseResolvePResponse> reverseResolve(
        alluxio.grpc.ReverseResolvePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReverseResolveMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Schedules async persistence.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.ScheduleAsyncPersistencePResponse> scheduleAsyncPersistence(
        alluxio.grpc.ScheduleAsyncPersistencePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScheduleAsyncPersistenceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Sets ACL for the path.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.SetAclPResponse> setAcl(
        alluxio.grpc.SetAclPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetAclMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Sets file or directory attributes.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.SetAttributePResponse> setAttribute(
        alluxio.grpc.SetAttributePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSetAttributeMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Start the active syncing of the directory or file
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.StartSyncPResponse> startSync(
        alluxio.grpc.StartSyncPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStartSyncMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Start the active syncing of the directory or file
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.StopSyncPResponse> stopSync(
        alluxio.grpc.StopSyncPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getStopSyncMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Deletes an existing "mount point", voiding the Alluxio namespace at the given path. The path
     * should correspond to an existing mount point. Any files in its subtree that are backed by UFS
     * will be persisted before they are removed from the Alluxio namespace.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.UnmountPResponse> unmount(
        alluxio.grpc.UnmountPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUnmountMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Updates an existing "mount point", changing its mount properties
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.UpdateMountPResponse> updateMount(
        alluxio.grpc.UpdateMountPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateMountMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Updates the ufs mode for a ufs path under one or more mount points.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.UpdateUfsModePResponse> updateUfsMode(
        alluxio.grpc.UpdateUfsModePRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateUfsModeMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetStateLockHoldersPResponse> getStateLockHolders(
        alluxio.grpc.GetStateLockHoldersPRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetStateLockHoldersMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CHECK_ACCESS = 0;
  private static final int METHODID_CHECK_CONSISTENCY = 1;
  private static final int METHODID_EXISTS = 2;
  private static final int METHODID_COMPLETE_FILE = 3;
  private static final int METHODID_CREATE_DIRECTORY = 4;
  private static final int METHODID_CREATE_FILE = 5;
  private static final int METHODID_FREE = 6;
  private static final int METHODID_FREE_WORKER = 7;
  private static final int METHODID_GET_FILE_PATH = 8;
  private static final int METHODID_GET_MOUNT_TABLE = 9;
  private static final int METHODID_GET_SYNC_PATH_LIST = 10;
  private static final int METHODID_GET_NEW_BLOCK_ID_FOR_FILE = 11;
  private static final int METHODID_GET_STATUS = 12;
  private static final int METHODID_LIST_STATUS = 13;
  private static final int METHODID_MOUNT = 14;
  private static final int METHODID_REMOVE = 15;
  private static final int METHODID_RENAME = 16;
  private static final int METHODID_REVERSE_RESOLVE = 17;
  private static final int METHODID_SCHEDULE_ASYNC_PERSISTENCE = 18;
  private static final int METHODID_SET_ACL = 19;
  private static final int METHODID_SET_ATTRIBUTE = 20;
  private static final int METHODID_START_SYNC = 21;
  private static final int METHODID_STOP_SYNC = 22;
  private static final int METHODID_UNMOUNT = 23;
  private static final int METHODID_UPDATE_MOUNT = 24;
  private static final int METHODID_UPDATE_UFS_MODE = 25;
  private static final int METHODID_GET_STATE_LOCK_HOLDERS = 26;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final FileSystemMasterClientServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(FileSystemMasterClientServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CHECK_ACCESS:
          serviceImpl.checkAccess((alluxio.grpc.CheckAccessPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CheckAccessPResponse>) responseObserver);
          break;
        case METHODID_CHECK_CONSISTENCY:
          serviceImpl.checkConsistency((alluxio.grpc.CheckConsistencyPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CheckConsistencyPResponse>) responseObserver);
          break;
        case METHODID_EXISTS:
          serviceImpl.exists((alluxio.grpc.ExistsPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.ExistsPResponse>) responseObserver);
          break;
        case METHODID_COMPLETE_FILE:
          serviceImpl.completeFile((alluxio.grpc.CompleteFilePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CompleteFilePResponse>) responseObserver);
          break;
        case METHODID_CREATE_DIRECTORY:
          serviceImpl.createDirectory((alluxio.grpc.CreateDirectoryPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CreateDirectoryPResponse>) responseObserver);
          break;
        case METHODID_CREATE_FILE:
          serviceImpl.createFile((alluxio.grpc.CreateFilePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CreateFilePResponse>) responseObserver);
          break;
        case METHODID_FREE:
          serviceImpl.free((alluxio.grpc.FreePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.FreePResponse>) responseObserver);
          break;
        case METHODID_FREE_WORKER:
          serviceImpl.freeWorker((alluxio.grpc.FreeWorkerPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.FreeWorkerPResponse>) responseObserver);
          break;
        case METHODID_GET_FILE_PATH:
          serviceImpl.getFilePath((alluxio.grpc.GetFilePathPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetFilePathPResponse>) responseObserver);
          break;
        case METHODID_GET_MOUNT_TABLE:
          serviceImpl.getMountTable((alluxio.grpc.GetMountTablePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetMountTablePResponse>) responseObserver);
          break;
        case METHODID_GET_SYNC_PATH_LIST:
          serviceImpl.getSyncPathList((alluxio.grpc.GetSyncPathListPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetSyncPathListPResponse>) responseObserver);
          break;
        case METHODID_GET_NEW_BLOCK_ID_FOR_FILE:
          serviceImpl.getNewBlockIdForFile((alluxio.grpc.GetNewBlockIdForFilePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetNewBlockIdForFilePResponse>) responseObserver);
          break;
        case METHODID_GET_STATUS:
          serviceImpl.getStatus((alluxio.grpc.GetStatusPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetStatusPResponse>) responseObserver);
          break;
        case METHODID_LIST_STATUS:
          serviceImpl.listStatus((alluxio.grpc.ListStatusPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.ListStatusPResponse>) responseObserver);
          break;
        case METHODID_MOUNT:
          serviceImpl.mount((alluxio.grpc.MountPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.MountPResponse>) responseObserver);
          break;
        case METHODID_REMOVE:
          serviceImpl.remove((alluxio.grpc.DeletePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.DeletePResponse>) responseObserver);
          break;
        case METHODID_RENAME:
          serviceImpl.rename((alluxio.grpc.RenamePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.RenamePResponse>) responseObserver);
          break;
        case METHODID_REVERSE_RESOLVE:
          serviceImpl.reverseResolve((alluxio.grpc.ReverseResolvePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.ReverseResolvePResponse>) responseObserver);
          break;
        case METHODID_SCHEDULE_ASYNC_PERSISTENCE:
          serviceImpl.scheduleAsyncPersistence((alluxio.grpc.ScheduleAsyncPersistencePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.ScheduleAsyncPersistencePResponse>) responseObserver);
          break;
        case METHODID_SET_ACL:
          serviceImpl.setAcl((alluxio.grpc.SetAclPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.SetAclPResponse>) responseObserver);
          break;
        case METHODID_SET_ATTRIBUTE:
          serviceImpl.setAttribute((alluxio.grpc.SetAttributePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.SetAttributePResponse>) responseObserver);
          break;
        case METHODID_START_SYNC:
          serviceImpl.startSync((alluxio.grpc.StartSyncPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.StartSyncPResponse>) responseObserver);
          break;
        case METHODID_STOP_SYNC:
          serviceImpl.stopSync((alluxio.grpc.StopSyncPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.StopSyncPResponse>) responseObserver);
          break;
        case METHODID_UNMOUNT:
          serviceImpl.unmount((alluxio.grpc.UnmountPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.UnmountPResponse>) responseObserver);
          break;
        case METHODID_UPDATE_MOUNT:
          serviceImpl.updateMount((alluxio.grpc.UpdateMountPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.UpdateMountPResponse>) responseObserver);
          break;
        case METHODID_UPDATE_UFS_MODE:
          serviceImpl.updateUfsMode((alluxio.grpc.UpdateUfsModePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.UpdateUfsModePResponse>) responseObserver);
          break;
        case METHODID_GET_STATE_LOCK_HOLDERS:
          serviceImpl.getStateLockHolders((alluxio.grpc.GetStateLockHoldersPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetStateLockHoldersPResponse>) responseObserver);
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

  private static abstract class FileSystemMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    FileSystemMasterClientServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return alluxio.grpc.FileSystemMasterProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("FileSystemMasterClientService");
    }
  }

  private static final class FileSystemMasterClientServiceFileDescriptorSupplier
      extends FileSystemMasterClientServiceBaseDescriptorSupplier {
    FileSystemMasterClientServiceFileDescriptorSupplier() {}
  }

  private static final class FileSystemMasterClientServiceMethodDescriptorSupplier
      extends FileSystemMasterClientServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    FileSystemMasterClientServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new FileSystemMasterClientServiceFileDescriptorSupplier())
              .addMethod(getCheckAccessMethod())
              .addMethod(getCheckConsistencyMethod())
              .addMethod(getExistsMethod())
              .addMethod(getCompleteFileMethod())
              .addMethod(getCreateDirectoryMethod())
              .addMethod(getCreateFileMethod())
              .addMethod(getFreeMethod())
              .addMethod(getFreeWorkerMethod())
              .addMethod(getGetFilePathMethod())
              .addMethod(getGetMountTableMethod())
              .addMethod(getGetSyncPathListMethod())
              .addMethod(getGetNewBlockIdForFileMethod())
              .addMethod(getGetStatusMethod())
              .addMethod(getListStatusMethod())
              .addMethod(getMountMethod())
              .addMethod(getRemoveMethod())
              .addMethod(getRenameMethod())
              .addMethod(getReverseResolveMethod())
              .addMethod(getScheduleAsyncPersistenceMethod())
              .addMethod(getSetAclMethod())
              .addMethod(getSetAttributeMethod())
              .addMethod(getStartSyncMethod())
              .addMethod(getStopSyncMethod())
              .addMethod(getUnmountMethod())
              .addMethod(getUpdateMountMethod())
              .addMethod(getUpdateUfsModeMethod())
              .addMethod(getGetStateLockHoldersMethod())
              .build();
        }
      }
    }
    return result;
  }
}
