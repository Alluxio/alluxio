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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.RestUtils;
import alluxio.client.file.FileSystemClientOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.MasterProcess;
import alluxio.master.file.options.CompleteFileContext;
import alluxio.master.file.options.CreateDirectoryContext;
import alluxio.master.file.options.CreateFileContext;
import alluxio.master.file.options.DeleteContext;
import alluxio.master.file.options.FreeContext;
import alluxio.master.file.options.MountContext;
import alluxio.master.file.options.RenameContext;
import alluxio.master.file.options.SetAttributeContext;
import alluxio.util.grpc.GrpcUtils;
import alluxio.web.MasterWebServer;
import alluxio.wire.FileInfo;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.TtlAction;

import com.google.common.base.Preconditions;
import com.qmino.miredot.annotations.ReturnType;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletContext;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for file system master requests.
 *
 * @deprecated since version 1.4 and will be removed in version 2.0
 */
@NotThreadSafe
@Path(FileSystemMasterClientRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
@Deprecated
public final class FileSystemMasterClientRestServiceHandler {
  public static final String SERVICE_PREFIX = "master/file";
  public static final String SERVICE_NAME = "service_name";
  public static final String SERVICE_VERSION = "service_version";
  public static final String COMPLETE_FILE = "complete_file";
  public static final String CREATE_DIRECTORY = "create_directory";
  public static final String CREATE_FILE = "create_file";
  public static final String FREE = "free";
  public static final String GET_MOUNT_POINTS = "mount_points";
  public static final String GET_NEW_BLOCK_ID_FOR_FILE = "new_block_id_for_file";
  public static final String GET_STATUS = "status";
  public static final String LIST_STATUS = "list_status";
  public static final String MOUNT = "mount";
  public static final String REMOVE = "remove";
  public static final String RENAME = "rename";
  public static final String SCHEDULE_ASYNC_PERSIST = "schedule_async_persist";
  public static final String SET_ATTRIBUTE = "set_attribute";
  public static final String UNMOUNT = "unmount";

  private final FileSystemMaster mFileSystemMaster;

  /**
   * Constructs a new {@link FileSystemMasterClientRestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public FileSystemMasterClientRestServiceHandler(@Context ServletContext context) {
    // Poor man's dependency injection through the Jersey application scope.
    mFileSystemMaster = ((MasterProcess) context
        .getAttribute(MasterWebServer.ALLUXIO_MASTER_SERVLET_RESOURCE_KEY))
        .getMaster(FileSystemMaster.class);
  }

  /**
   * @summary get the service name
   * @return the response object
   */
  @GET
  @Path(SERVICE_NAME)
  @ReturnType("java.lang.String")
  public Response getServiceName() {
    return RestUtils.call(() -> Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME);
  }

  /**
   * @summary get the service version
   * @return the response object
   */
  @GET
  @Path(SERVICE_VERSION)
  @ReturnType("java.lang.Long")
  public Response getServiceVersion() {
    return RestUtils.call(() -> Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION);
  }

  /**
   * @summary complete a file
   * @param path the file path
   * @param ufsLength the length of the file in under file system
   * @return the response object
   */
  @POST
  @Path(COMPLETE_FILE)
  @ReturnType("java.lang.Void")
  public Response completeFile(@QueryParam("path") final String path,
      @QueryParam("ufsLength") final Long ufsLength) {
    return RestUtils.call((RestUtils.RestCallable<Void>) () -> {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      CompleteFileContext context = CompleteFileContext.defaults();
      if (ufsLength != null) {
        context.getOptions().setUfsLength(ufsLength);
      }
      mFileSystemMaster.completeFile(new AlluxioURI(path), context);
      return null;
    });
  }

  /**
   * @summary create a directory
   * @param path the file path
   * @param persisted whether directory should be persisted
   * @param recursive whether parent directories should be created if they do not already exist
   * @param allowExists whether the operation should succeed even if the directory already exists
   * @return the response object
   */
  @POST
  @Path(CREATE_DIRECTORY)
  @ReturnType("java.lang.Void")
  public Response createDirectory(@QueryParam("path") final String path,
      @QueryParam("persisted") final Boolean persisted,
      @QueryParam("recursive") final Boolean recursive,
      @QueryParam("allowExists") final Boolean allowExists) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        Preconditions.checkNotNull(path, "required 'path' parameter is missing");
        CreateDirectoryContext context = CreateDirectoryContext.defaults();
        if (persisted != null) {
          context.getOptions().setPersisted(persisted);
        }
        if (recursive != null) {
          context.getOptions().setRecursive(recursive);
        }
        if (allowExists != null) {
          context.getOptions().setAllowExist(allowExists);
        }
        mFileSystemMaster.createDirectory(new AlluxioURI(path), context);
        return null;
      }
    });
  }

  /**
   * @summary create a file
   * @param path the file path
   * @param persisted whether directory should be persisted
   * @param recursive whether parent directories should be created if they do not already exist
   * @param blockSizeBytes the target block size in bytes
   * @param ttl the time-to-live (in milliseconds)
   * @param ttlAction action to take after TTL is expired
   * @return the response object
   */
  @POST
  @Path(CREATE_FILE)
  @ReturnType("java.lang.Void")
  public Response createFile(@QueryParam("path") final String path,
      @QueryParam("persisted") final Boolean persisted,
      @QueryParam("recursive") final Boolean recursive,
      @QueryParam("blockSizeBytes") final Long blockSizeBytes, @QueryParam("ttl") final Long ttl,
      @QueryParam("ttlAction") final TtlAction ttlAction) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        Preconditions.checkNotNull(path, "required 'path' parameter is missing");
        CreateFileContext context = CreateFileContext.defaults();
        if (persisted != null) {
          context.getOptions().setPersisted(persisted);
        }
        if (recursive != null) {
            context.getOptions().setRecursive(recursive);
        }
        if (blockSizeBytes != null) {
            context.getOptions().setBlockSizeBytes(blockSizeBytes);
        }
        if (ttl != null) {
          FileSystemMasterCommonPOptions.Builder commonOptions =
              FileSystemMasterCommonPOptions.newBuilder();
          commonOptions.setTtl(ttl);
          if (ttlAction != null) {
            commonOptions.setTtlAction(GrpcUtils.toProto(ttlAction));
          }
          context.getOptions().setCommonOptions(commonOptions);
        }
        mFileSystemMaster.createFile(new AlluxioURI(path), context);
        return null;
      }
    });
  }

  /**
   * @summary get a new block id for a file
   * @param path the file path
   * @return the response object
   */
  @POST
  @Path(GET_NEW_BLOCK_ID_FOR_FILE)
  @ReturnType("java.lang.Long")
  public Response getNewBlockIdForFile(@QueryParam("path") final String path) {
    return RestUtils.call(() -> {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      return mFileSystemMaster.getNewBlockIdForFile(new AlluxioURI(path));
    });
  }
  /**
   * @summary get a file descriptor for a path
   * @param path the file path
   * @return the response object
   */
  @GET
  @Path(GET_STATUS)
  @ReturnType("alluxio.wire.FileInfo")
  public Response getStatus(@QueryParam("path") final String path) {
    return RestUtils.call(() -> {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      return mFileSystemMaster.getFileInfo(new AlluxioURI(path), null);
//      return mFileSystemMaster.getFileInfo(new AlluxioURI(path), GetStatusOptions.defaults());
    });
  }

  /**
   * @summary free a path
   * @param path the path
   * @param recursive whether the path should be freed recursively
   * @return the response object
   */
  @POST
  @Path(FREE)
  @ReturnType("java.lang.Void")
  public Response free(@QueryParam("path") final String path,
      @QueryParam("recursive") final boolean recursive) {
    return RestUtils.call((RestUtils.RestCallable<Void>) () -> {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      mFileSystemMaster.free(new AlluxioURI(path),
          FreeContext.defaults(FreePOptions.newBuilder().setRecursive(recursive)));
      return null;
    });
  }

  /**
   * @summary get the file descriptors for a path
   * @param path the file path
   * @param loadDirectChildren whether to load direct children of path
   * @param loadMetadataType the {@link LoadMetadataType}. It overrides loadDirectChildren if it
   *        is set.
   * @return the response object
   */
  @GET
  @Path(LIST_STATUS)
  @ReturnType("java.util.List<alluxio.wire.FileInfo>")
  public Response listStatus(@QueryParam("path") final String path,
      @Deprecated @QueryParam("loadDirectChildren") final boolean loadDirectChildren,
      @DefaultValue("") @QueryParam("loadMetadataType") final String loadMetadataType) {
    return RestUtils.call(new RestUtils.RestCallable<List<FileInfo>>() {
      @Override
      public List<FileInfo> call() throws Exception {
        Preconditions.checkNotNull(path, "required 'path' parameter is missing");
        ListStatusPOptions.Builder listStatusOptionsBuilder =
            FileSystemClientOptions.getListStatusOptions().toBuilder();
        if (!loadDirectChildren) {
          listStatusOptionsBuilder.setLoadMetadataType(LoadMetadataPType.NEVER);
        }
        // loadMetadataType overrides loadDirectChildren if it is set.
        if (!loadMetadataType.isEmpty()) {
          listStatusOptionsBuilder.setLoadMetadataType(
              GrpcUtils.toProto(LoadMetadataType.valueOf(loadMetadataType)));
        }
        return mFileSystemMaster.listStatus(new AlluxioURI(path),
            listStatusOptionsBuilder.build());
      }
    });
  }

  /**
   * @summary mount a UFS path
   * @param path the alluxio mount point
   * @param ufsPath the UFS path to mount
   * @param readOnly whether to make the mount option read only
   * @param shared whether to make the mount option shared with all Alluxio users
   * @return the response object
   */
  @POST
  @Path(MOUNT)
  @ReturnType("java.lang.Void")
  public Response mount(@QueryParam("path") final String path,
      @QueryParam("ufsPath") final String ufsPath, @QueryParam("readOnly") final Boolean readOnly,
      @QueryParam("shared") final Boolean shared) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        Preconditions.checkNotNull(path, "required 'path' parameter is missing");
        Preconditions.checkNotNull(ufsPath, "required 'ufsPath' parameter is missing");
        MountPOptions.Builder optionsBuilder = MountContext.defaults().getOptions();
        if (readOnly != null) {
          optionsBuilder.setReadOnly(readOnly);
        }
        if (shared != null) {
          optionsBuilder.setShared(shared);
        }
        mFileSystemMaster.mount(new AlluxioURI(path), new AlluxioURI(ufsPath),
            MountContext.defaults(optionsBuilder));
        return null;
      }
    });
  }

  /**
   * @summary get the map from alluxio paths of mount points to the mount point details
   * @return the response object
   */
  @GET
  @Path(GET_MOUNT_POINTS)
  @ReturnType("java.util.SortedMap<java.lang.String, alluxio.wire.MountPointInfo>")
  public Response getMountPoints() {
    return RestUtils.call(() -> mFileSystemMaster.getMountTable());
  }

  /**
   * @summary remove a path
   * @param path the path to remove
   * @param recursive whether to remove paths recursively
   * @return the response object
   */
  @POST
  @Path(REMOVE)
  @ReturnType("java.lang.Void")
  public Response remove(@QueryParam("path") final String path,
      @QueryParam("recursive") final boolean recursive) {
    return RestUtils.call((RestUtils.RestCallable<Void>) () -> {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      mFileSystemMaster.delete(new AlluxioURI(path),
          DeleteContext.defaults(DeletePOptions.newBuilder().setRecursive(recursive)));
      return null;
    });
  }

  /**
   * @summary move a path
   * @param srcPath the source path
   * @param dstPath the destination path
   * @return the response object
   */
  @POST
  @Path(RENAME)
  @ReturnType("java.lang.Void")
  public Response rename(@QueryParam("srcPath") final String srcPath,
      @QueryParam("dstPath") final String dstPath) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        Preconditions.checkNotNull(srcPath, "required 'srcPath' parameter is missing");
        Preconditions.checkNotNull(dstPath, "required 'dstPath' parameter is missing");
        mFileSystemMaster.rename(new AlluxioURI(srcPath), new AlluxioURI(dstPath),
            RenameContext.defaults());
        return null;
      }
    });
  }

  /**
   * @summary schedule asynchronous persistence
   * @param path the file path
   * @return the response object
   */
  @POST
  @Path(SCHEDULE_ASYNC_PERSIST)
  @ReturnType("java.lang.Void")
  public Response scheduleAsyncPersist(@QueryParam("path") final String path) {
    return RestUtils.call((RestUtils.RestCallable<Void>) () -> {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      mFileSystemMaster.scheduleAsyncPersistence(new AlluxioURI(path));
      return null;
    });
  }

  /**
   * @summary set an attribute
   * @param path the file path
   * @param pinned the pinned flag value to use
   * @param ttl the time-to-live (in seconds) to use
   * @param persisted the persisted flag value to use
   * @param owner the file owner
   * @param group the file group
   * @param permission the file permission bits
   * @param recursive whether the attribute should be set recursively
   * @param ttlAction action to take after TTL is expired
   * @return the response object
   */
  @POST
  @Path(SET_ATTRIBUTE)
  @ReturnType("java.lang.Void")
  public Response setAttribute(@QueryParam("path") final String path,
      @QueryParam("pinned") final Boolean pinned, @QueryParam("ttl") final Long ttl,
      @QueryParam("persisted") final Boolean persisted, @QueryParam("owner") final String owner,
      @QueryParam("group") final String group, @QueryParam("permission") final Short permission,
      @QueryParam("recursive") final Boolean recursive,
      @QueryParam("ttxAction") final TtlAction ttlAction) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        SetAttributePOptions.Builder optionsBuilder = SetAttributePOptions.newBuilder();

        Preconditions.checkNotNull(path, "required 'path' parameter is missing");
        if (pinned != null) {
          optionsBuilder.setPinned(pinned);
        }
        if (ttl != null) {
          optionsBuilder.setTtl(ttl);
        }
        if (ttlAction != null) {
          optionsBuilder.setTtlAction(GrpcUtils.toProto(ttlAction));
        }
        if (persisted != null) {
          optionsBuilder.setPersisted(persisted);
        }
        if (owner != null) {
          optionsBuilder.setOwner(owner);
        }
        if (group != null) {
          optionsBuilder.setGroup(group);
        }
        if (permission != null) {
          optionsBuilder.setMode(permission);
        }
        if (recursive != null) {
          optionsBuilder.setRecursive(recursive);
        }
        mFileSystemMaster.setAttribute(new AlluxioURI(path),
            SetAttributeContext.defaults(optionsBuilder));
        return null;
      }
    });
  }

  /**
   * @summary unmount a path
   * @param path the file path
   * @return the response object
   */
  @POST
  @Path(UNMOUNT)
  @ReturnType("java.lang.Void")
  public Response unmount(@QueryParam("path") final String path) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        Preconditions.checkNotNull(path, "required 'path' parameter is missing");
        mFileSystemMaster.unmount(new AlluxioURI(path));
        return null;
      }
    });
  }
}
