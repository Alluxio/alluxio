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
 * This interface contains file system master service endpoints for Alluxio clients.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.17.1)",
    comments = "Source: grpc/file_system_master.proto")
public final class FileSystemMasterClientServiceGrpc {

  private FileSystemMasterClientServiceGrpc() {}

  public static final String SERVICE_NAME = "alluxio.grpc.file.FileSystemMasterClientService";

  // Static method descriptors that strictly reflect the proto.
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "CheckConsistency"))
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "CompleteFile"))
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "CreateDirectory"))
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "CreateFile"))
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "Free"))
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "GetMountTable"))
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "GetNewBlockIdForFile"))
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "GetStatus"))
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
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<alluxio.grpc.ListStatusPRequest,
      alluxio.grpc.ListStatusPResponse> getListStatusMethod() {
    io.grpc.MethodDescriptor<alluxio.grpc.ListStatusPRequest, alluxio.grpc.ListStatusPResponse> getListStatusMethod;
    if ((getListStatusMethod = FileSystemMasterClientServiceGrpc.getListStatusMethod) == null) {
      synchronized (FileSystemMasterClientServiceGrpc.class) {
        if ((getListStatusMethod = FileSystemMasterClientServiceGrpc.getListStatusMethod) == null) {
          FileSystemMasterClientServiceGrpc.getListStatusMethod = getListStatusMethod = 
              io.grpc.MethodDescriptor.<alluxio.grpc.ListStatusPRequest, alluxio.grpc.ListStatusPResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "ListStatus"))
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "Mount"))
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "Remove"))
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "Rename"))
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "ScheduleAsyncPersistence"))
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "SetAcl"))
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "SetAttribute"))
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "Unmount"))
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
              .setFullMethodName(generateFullMethodName(
                  "alluxio.grpc.file.FileSystemMasterClientService", "UpdateUfsMode"))
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

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static FileSystemMasterClientServiceStub newStub(io.grpc.Channel channel) {
    return new FileSystemMasterClientServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static FileSystemMasterClientServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new FileSystemMasterClientServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static FileSystemMasterClientServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new FileSystemMasterClientServiceFutureStub(channel);
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
     * Checks the consistency of the files and directores with the path as the root of the subtree
     * </pre>
     */
    public void checkConsistency(alluxio.grpc.CheckConsistencyPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CheckConsistencyPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCheckConsistencyMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Marks a file as completed.
     * </pre>
     */
    public void completeFile(alluxio.grpc.CompleteFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CompleteFilePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCompleteFileMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Creates a directory.
     * </pre>
     */
    public void createDirectory(alluxio.grpc.CreateDirectoryPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateDirectoryPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCreateDirectoryMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Creates a file.
     * </pre>
     */
    public void createFile(alluxio.grpc.CreateFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CreateFilePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCreateFileMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Frees the given file or directory from Alluxio.
     * </pre>
     */
    public void free(alluxio.grpc.FreePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.FreePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getFreeMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a map from each Alluxio path to information of corresponding mount point
     * </pre>
     */
    public void getMountTable(alluxio.grpc.GetMountTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetMountTablePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetMountTableMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Generates a new block id for the given file.
     * </pre>
     */
    public void getNewBlockIdForFile(alluxio.grpc.GetNewBlockIdForFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetNewBlockIdForFilePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetNewBlockIdForFileMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Returns the status of the file or directory.
     * </pre>
     */
    public void getStatus(alluxio.grpc.GetStatusPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetStatusPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getGetStatusMethod(), responseObserver);
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
      asyncUnimplementedUnaryCall(getListStatusMethod(), responseObserver);
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
      asyncUnimplementedUnaryCall(getMountMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Deletes a file or a directory and returns whether the remove operation succeeded.
     * NOTUnfortunately, the method cannot be called "delete" as that is a reserved Thrift keyword.
     * </pre>
     */
    public void remove(alluxio.grpc.DeletePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.DeletePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRemoveMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Renames a file or a directory.
     * </pre>
     */
    public void rename(alluxio.grpc.RenamePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.RenamePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getRenameMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Schedules async persistence.
     * </pre>
     */
    public void scheduleAsyncPersistence(alluxio.grpc.ScheduleAsyncPersistencePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ScheduleAsyncPersistencePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getScheduleAsyncPersistenceMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Sets ACL for the path.
     * </pre>
     */
    public void setAcl(alluxio.grpc.SetAclPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.SetAclPResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSetAclMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Sets file or directory attributes.
     * </pre>
     */
    public void setAttribute(alluxio.grpc.SetAttributePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.SetAttributePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getSetAttributeMethod(), responseObserver);
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
      asyncUnimplementedUnaryCall(getUnmountMethod(), responseObserver);
    }

    /**
     * <pre>
     **
     * Updates the ufs mode for a ufs path under one or more mount points.
     * </pre>
     */
    public void updateUfsMode(alluxio.grpc.UpdateUfsModePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.UpdateUfsModePResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getUpdateUfsModeMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCheckConsistencyMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CheckConsistencyPRequest,
                alluxio.grpc.CheckConsistencyPResponse>(
                  this, METHODID_CHECK_CONSISTENCY)))
          .addMethod(
            getCompleteFileMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CompleteFilePRequest,
                alluxio.grpc.CompleteFilePResponse>(
                  this, METHODID_COMPLETE_FILE)))
          .addMethod(
            getCreateDirectoryMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CreateDirectoryPRequest,
                alluxio.grpc.CreateDirectoryPResponse>(
                  this, METHODID_CREATE_DIRECTORY)))
          .addMethod(
            getCreateFileMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.CreateFilePRequest,
                alluxio.grpc.CreateFilePResponse>(
                  this, METHODID_CREATE_FILE)))
          .addMethod(
            getFreeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.FreePRequest,
                alluxio.grpc.FreePResponse>(
                  this, METHODID_FREE)))
          .addMethod(
            getGetMountTableMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetMountTablePRequest,
                alluxio.grpc.GetMountTablePResponse>(
                  this, METHODID_GET_MOUNT_TABLE)))
          .addMethod(
            getGetNewBlockIdForFileMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetNewBlockIdForFilePRequest,
                alluxio.grpc.GetNewBlockIdForFilePResponse>(
                  this, METHODID_GET_NEW_BLOCK_ID_FOR_FILE)))
          .addMethod(
            getGetStatusMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.GetStatusPRequest,
                alluxio.grpc.GetStatusPResponse>(
                  this, METHODID_GET_STATUS)))
          .addMethod(
            getListStatusMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.ListStatusPRequest,
                alluxio.grpc.ListStatusPResponse>(
                  this, METHODID_LIST_STATUS)))
          .addMethod(
            getMountMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.MountPRequest,
                alluxio.grpc.MountPResponse>(
                  this, METHODID_MOUNT)))
          .addMethod(
            getRemoveMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.DeletePRequest,
                alluxio.grpc.DeletePResponse>(
                  this, METHODID_REMOVE)))
          .addMethod(
            getRenameMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.RenamePRequest,
                alluxio.grpc.RenamePResponse>(
                  this, METHODID_RENAME)))
          .addMethod(
            getScheduleAsyncPersistenceMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.ScheduleAsyncPersistencePRequest,
                alluxio.grpc.ScheduleAsyncPersistencePResponse>(
                  this, METHODID_SCHEDULE_ASYNC_PERSISTENCE)))
          .addMethod(
            getSetAclMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.SetAclPRequest,
                alluxio.grpc.SetAclPResponse>(
                  this, METHODID_SET_ACL)))
          .addMethod(
            getSetAttributeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.SetAttributePRequest,
                alluxio.grpc.SetAttributePResponse>(
                  this, METHODID_SET_ATTRIBUTE)))
          .addMethod(
            getUnmountMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.UnmountPRequest,
                alluxio.grpc.UnmountPResponse>(
                  this, METHODID_UNMOUNT)))
          .addMethod(
            getUpdateUfsModeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                alluxio.grpc.UpdateUfsModePRequest,
                alluxio.grpc.UpdateUfsModePResponse>(
                  this, METHODID_UPDATE_UFS_MODE)))
          .build();
    }
  }

  /**
   * <pre>
   **
   * This interface contains file system master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class FileSystemMasterClientServiceStub extends io.grpc.stub.AbstractStub<FileSystemMasterClientServiceStub> {
    private FileSystemMasterClientServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileSystemMasterClientServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemMasterClientServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileSystemMasterClientServiceStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Checks the consistency of the files and directores with the path as the root of the subtree
     * </pre>
     */
    public void checkConsistency(alluxio.grpc.CheckConsistencyPRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CheckConsistencyPResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCheckConsistencyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Marks a file as completed.
     * </pre>
     */
    public void completeFile(alluxio.grpc.CompleteFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.CompleteFilePResponse> responseObserver) {
      asyncUnaryCall(
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
      asyncUnaryCall(
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
      asyncUnaryCall(
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
      asyncUnaryCall(
          getChannel().newCall(getFreeMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Returns a map from each Alluxio path to information of corresponding mount point
     * </pre>
     */
    public void getMountTable(alluxio.grpc.GetMountTablePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetMountTablePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetMountTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Generates a new block id for the given file.
     * </pre>
     */
    public void getNewBlockIdForFile(alluxio.grpc.GetNewBlockIdForFilePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.GetNewBlockIdForFilePResponse> responseObserver) {
      asyncUnaryCall(
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
      asyncUnaryCall(
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
      asyncUnaryCall(
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
      asyncUnaryCall(
          getChannel().newCall(getMountMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Deletes a file or a directory and returns whether the remove operation succeeded.
     * NOTUnfortunately, the method cannot be called "delete" as that is a reserved Thrift keyword.
     * </pre>
     */
    public void remove(alluxio.grpc.DeletePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.DeletePResponse> responseObserver) {
      asyncUnaryCall(
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
      asyncUnaryCall(
          getChannel().newCall(getRenameMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Schedules async persistence.
     * </pre>
     */
    public void scheduleAsyncPersistence(alluxio.grpc.ScheduleAsyncPersistencePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.ScheduleAsyncPersistencePResponse> responseObserver) {
      asyncUnaryCall(
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
      asyncUnaryCall(
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
      asyncUnaryCall(
          getChannel().newCall(getSetAttributeMethod(), getCallOptions()), request, responseObserver);
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
      asyncUnaryCall(
          getChannel().newCall(getUnmountMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     **
     * Updates the ufs mode for a ufs path under one or more mount points.
     * </pre>
     */
    public void updateUfsMode(alluxio.grpc.UpdateUfsModePRequest request,
        io.grpc.stub.StreamObserver<alluxio.grpc.UpdateUfsModePResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getUpdateUfsModeMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   * This interface contains file system master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class FileSystemMasterClientServiceBlockingStub extends io.grpc.stub.AbstractStub<FileSystemMasterClientServiceBlockingStub> {
    private FileSystemMasterClientServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileSystemMasterClientServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemMasterClientServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileSystemMasterClientServiceBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Checks the consistency of the files and directores with the path as the root of the subtree
     * </pre>
     */
    public alluxio.grpc.CheckConsistencyPResponse checkConsistency(alluxio.grpc.CheckConsistencyPRequest request) {
      return blockingUnaryCall(
          getChannel(), getCheckConsistencyMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Marks a file as completed.
     * </pre>
     */
    public alluxio.grpc.CompleteFilePResponse completeFile(alluxio.grpc.CompleteFilePRequest request) {
      return blockingUnaryCall(
          getChannel(), getCompleteFileMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Creates a directory.
     * </pre>
     */
    public alluxio.grpc.CreateDirectoryPResponse createDirectory(alluxio.grpc.CreateDirectoryPRequest request) {
      return blockingUnaryCall(
          getChannel(), getCreateDirectoryMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Creates a file.
     * </pre>
     */
    public alluxio.grpc.CreateFilePResponse createFile(alluxio.grpc.CreateFilePRequest request) {
      return blockingUnaryCall(
          getChannel(), getCreateFileMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Frees the given file or directory from Alluxio.
     * </pre>
     */
    public alluxio.grpc.FreePResponse free(alluxio.grpc.FreePRequest request) {
      return blockingUnaryCall(
          getChannel(), getFreeMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns a map from each Alluxio path to information of corresponding mount point
     * </pre>
     */
    public alluxio.grpc.GetMountTablePResponse getMountTable(alluxio.grpc.GetMountTablePRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetMountTableMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Generates a new block id for the given file.
     * </pre>
     */
    public alluxio.grpc.GetNewBlockIdForFilePResponse getNewBlockIdForFile(alluxio.grpc.GetNewBlockIdForFilePRequest request) {
      return blockingUnaryCall(
          getChannel(), getGetNewBlockIdForFileMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Returns the status of the file or directory.
     * </pre>
     */
    public alluxio.grpc.GetStatusPResponse getStatus(alluxio.grpc.GetStatusPRequest request) {
      return blockingUnaryCall(
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
    public alluxio.grpc.ListStatusPResponse listStatus(alluxio.grpc.ListStatusPRequest request) {
      return blockingUnaryCall(
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
      return blockingUnaryCall(
          getChannel(), getMountMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Deletes a file or a directory and returns whether the remove operation succeeded.
     * NOTUnfortunately, the method cannot be called "delete" as that is a reserved Thrift keyword.
     * </pre>
     */
    public alluxio.grpc.DeletePResponse remove(alluxio.grpc.DeletePRequest request) {
      return blockingUnaryCall(
          getChannel(), getRemoveMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Renames a file or a directory.
     * </pre>
     */
    public alluxio.grpc.RenamePResponse rename(alluxio.grpc.RenamePRequest request) {
      return blockingUnaryCall(
          getChannel(), getRenameMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Schedules async persistence.
     * </pre>
     */
    public alluxio.grpc.ScheduleAsyncPersistencePResponse scheduleAsyncPersistence(alluxio.grpc.ScheduleAsyncPersistencePRequest request) {
      return blockingUnaryCall(
          getChannel(), getScheduleAsyncPersistenceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Sets ACL for the path.
     * </pre>
     */
    public alluxio.grpc.SetAclPResponse setAcl(alluxio.grpc.SetAclPRequest request) {
      return blockingUnaryCall(
          getChannel(), getSetAclMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Sets file or directory attributes.
     * </pre>
     */
    public alluxio.grpc.SetAttributePResponse setAttribute(alluxio.grpc.SetAttributePRequest request) {
      return blockingUnaryCall(
          getChannel(), getSetAttributeMethod(), getCallOptions(), request);
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
      return blockingUnaryCall(
          getChannel(), getUnmountMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     **
     * Updates the ufs mode for a ufs path under one or more mount points.
     * </pre>
     */
    public alluxio.grpc.UpdateUfsModePResponse updateUfsMode(alluxio.grpc.UpdateUfsModePRequest request) {
      return blockingUnaryCall(
          getChannel(), getUpdateUfsModeMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   * This interface contains file system master service endpoints for Alluxio clients.
   * </pre>
   */
  public static final class FileSystemMasterClientServiceFutureStub extends io.grpc.stub.AbstractStub<FileSystemMasterClientServiceFutureStub> {
    private FileSystemMasterClientServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private FileSystemMasterClientServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected FileSystemMasterClientServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new FileSystemMasterClientServiceFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     **
     * Checks the consistency of the files and directores with the path as the root of the subtree
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CheckConsistencyPResponse> checkConsistency(
        alluxio.grpc.CheckConsistencyPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCheckConsistencyMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Marks a file as completed.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.CompleteFilePResponse> completeFile(
        alluxio.grpc.CompleteFilePRequest request) {
      return futureUnaryCall(
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
      return futureUnaryCall(
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
      return futureUnaryCall(
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
      return futureUnaryCall(
          getChannel().newCall(getFreeMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Returns a map from each Alluxio path to information of corresponding mount point
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetMountTablePResponse> getMountTable(
        alluxio.grpc.GetMountTablePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetMountTableMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Generates a new block id for the given file.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.GetNewBlockIdForFilePResponse> getNewBlockIdForFile(
        alluxio.grpc.GetNewBlockIdForFilePRequest request) {
      return futureUnaryCall(
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
      return futureUnaryCall(
          getChannel().newCall(getGetStatusMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * If the path points to a file, the method returns a singleton with its file information.
     * If the path points to a directory, the method returns a list with file information for the
     * directory contents.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.ListStatusPResponse> listStatus(
        alluxio.grpc.ListStatusPRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getListStatusMethod(), getCallOptions()), request);
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
      return futureUnaryCall(
          getChannel().newCall(getMountMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Deletes a file or a directory and returns whether the remove operation succeeded.
     * NOTUnfortunately, the method cannot be called "delete" as that is a reserved Thrift keyword.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.DeletePResponse> remove(
        alluxio.grpc.DeletePRequest request) {
      return futureUnaryCall(
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
      return futureUnaryCall(
          getChannel().newCall(getRenameMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Schedules async persistence.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.ScheduleAsyncPersistencePResponse> scheduleAsyncPersistence(
        alluxio.grpc.ScheduleAsyncPersistencePRequest request) {
      return futureUnaryCall(
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
      return futureUnaryCall(
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
      return futureUnaryCall(
          getChannel().newCall(getSetAttributeMethod(), getCallOptions()), request);
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
      return futureUnaryCall(
          getChannel().newCall(getUnmountMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     **
     * Updates the ufs mode for a ufs path under one or more mount points.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<alluxio.grpc.UpdateUfsModePResponse> updateUfsMode(
        alluxio.grpc.UpdateUfsModePRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getUpdateUfsModeMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CHECK_CONSISTENCY = 0;
  private static final int METHODID_COMPLETE_FILE = 1;
  private static final int METHODID_CREATE_DIRECTORY = 2;
  private static final int METHODID_CREATE_FILE = 3;
  private static final int METHODID_FREE = 4;
  private static final int METHODID_GET_MOUNT_TABLE = 5;
  private static final int METHODID_GET_NEW_BLOCK_ID_FOR_FILE = 6;
  private static final int METHODID_GET_STATUS = 7;
  private static final int METHODID_LIST_STATUS = 8;
  private static final int METHODID_MOUNT = 9;
  private static final int METHODID_REMOVE = 10;
  private static final int METHODID_RENAME = 11;
  private static final int METHODID_SCHEDULE_ASYNC_PERSISTENCE = 12;
  private static final int METHODID_SET_ACL = 13;
  private static final int METHODID_SET_ATTRIBUTE = 14;
  private static final int METHODID_UNMOUNT = 15;
  private static final int METHODID_UPDATE_UFS_MODE = 16;

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
        case METHODID_CHECK_CONSISTENCY:
          serviceImpl.checkConsistency((alluxio.grpc.CheckConsistencyPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.CheckConsistencyPResponse>) responseObserver);
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
        case METHODID_GET_MOUNT_TABLE:
          serviceImpl.getMountTable((alluxio.grpc.GetMountTablePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.GetMountTablePResponse>) responseObserver);
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
        case METHODID_UNMOUNT:
          serviceImpl.unmount((alluxio.grpc.UnmountPRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.UnmountPResponse>) responseObserver);
          break;
        case METHODID_UPDATE_UFS_MODE:
          serviceImpl.updateUfsMode((alluxio.grpc.UpdateUfsModePRequest) request,
              (io.grpc.stub.StreamObserver<alluxio.grpc.UpdateUfsModePResponse>) responseObserver);
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
              .addMethod(getCheckConsistencyMethod())
              .addMethod(getCompleteFileMethod())
              .addMethod(getCreateDirectoryMethod())
              .addMethod(getCreateFileMethod())
              .addMethod(getFreeMethod())
              .addMethod(getGetMountTableMethod())
              .addMethod(getGetNewBlockIdForFileMethod())
              .addMethod(getGetStatusMethod())
              .addMethod(getListStatusMethod())
              .addMethod(getMountMethod())
              .addMethod(getRemoveMethod())
              .addMethod(getRenameMethod())
              .addMethod(getScheduleAsyncPersistenceMethod())
              .addMethod(getSetAclMethod())
              .addMethod(getSetAttributeMethod())
              .addMethod(getUnmountMethod())
              .addMethod(getUpdateUfsModeMethod())
              .build();
        }
      }
    }
    return result;
  }
}
