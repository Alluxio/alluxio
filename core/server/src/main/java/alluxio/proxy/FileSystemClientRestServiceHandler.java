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

package alluxio.proxy;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.RestUtils;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.SetAttributeOptions;
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
 * This class is a REST handler for file system client requests.
 */
@NotThreadSafe
@Path(FileSystemClientRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class FileSystemClientRestServiceHandler {
  public static final String SERVICE_PREFIX = "file";
  public static final String SERVICE_NAME = "service_name";
  public static final String SERVICE_VERSION = "service_version";
  public static final String CREATE_DIRECTORY = "create_directory";
  public static final String DELETE = "delete";
  public static final String DOWNLOAD = "download";
  public static final String EXISTS = "exists";
  public static final String FREE = "free";
  public static final String GET_STATUS = "status";
  public static final String LIST_STATUS = "list_status";
  public static final String MOUNT = "mount";
  public static final String RENAME = "rename";
  public static final String SET_ATTRIBUTE = "set_attribute";
  public static final String UNMOUNT = "unmount";
  public static final String UPLOAD = "upload";

  private final FileSystem mFileSystem;

  /**
   * Constructs a new {@link FileSystemClientRestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public FileSystemClientRestServiceHandler(@Context ServletContext context) {
    mFileSystem = FileSystem.Factory.get();
  }

  /**
   * @summary get the service name
   * @return the response object
   */
  @GET
  @Path(SERVICE_NAME)
  @ReturnType("java.lang.String")
  public Response getServiceName() {
    return RestUtils.call(new RestUtils.RestCallable<String>() {
      @Override
      public String call() throws Exception {
        return Constants.FILE_SYSTEM_CLIENT_SERVICE_NAME;
      }
    });
  }

  /**
   * @summary get the service version
   * @return the response object
   */
  @GET
  @Path(SERVICE_VERSION)
  @ReturnType("java.lang.Long")
  public Response getServiceVersion() {
    return RestUtils.call(new RestUtils.RestCallable<Long>() {
      @Override
      public Long call() throws Exception {
        return Constants.FILE_SYSTEM_CLIENT_SERVICE_VERSION;
      }
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
        CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
        if (persisted != null) {
          options.setWriteType(
              persisted.booleanValue() ? WriteType.CACHE_THROUGH : WriteType.MUST_CACHE);
        }
        if (recursive != null) {
          options.setRecursive(recursive);
        }
        if (allowExists != null) {
          options.setAllowExists(allowExists);
        }
        // TODO(jsimsa): Support mode.
        mFileSystem.createDirectory(new AlluxioURI(path), options);
        return null;
      }
    });
  }

  /**
   * @summary remove a path
   * @param path the path to remove
   * @param recursive whether to remove paths recursively
   * @return the response object
   */
  @POST
  @Path(DELETE)
  @ReturnType("java.lang.Void")
  public Response remove(@QueryParam("path") final String path,
      @QueryParam("recursive") final boolean recursive) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        Preconditions.checkNotNull(path, "required 'path' parameter is missing");
        DeleteOptions options = DeleteOptions.defaults().setRecursive(recursive);
        mFileSystem.delete(new AlluxioURI(path), options);
        return null;
      }
    });
  }

  /**
   * @summary download a file
   * @param path the file path
   * @return the response object
   */
  @POST
  @Path(DOWNLOAD)
  @ReturnType("java.lang.Void")
  public Response download(@QueryParam("path") final String path) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        // TODO(jsimsa): Implement upload.
        // TODO(jsimsa): Support mode.
        return null;
      }
    });
  }

  /**
   * @summary checks whether a path exists
   * @param path the path
   * @return the response object
   */
  @POST
  @Path(EXISTS)
  @ReturnType("java.lang.Boolean")
  public Response exists(@QueryParam("path") final String path) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        Preconditions.checkNotNull(path, "required 'path' parameter is missing");
        mFileSystem.exists(new AlluxioURI(path));
        return null;
      }
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
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        Preconditions.checkNotNull(path, "required 'path' parameter is missing");
        FreeOptions options = FreeOptions.defaults().setRecursive(recursive);
        mFileSystem.free(new AlluxioURI(path), options);
        return null;
      }
    });
  }

  /**
   * @summary get a file descriptor for a path
   * @param path the file path
   * @return the response object
   */
  @GET
  @Path(GET_STATUS)
  @ReturnType("alluxio.client.file.URIStatus")
  public Response getStatus(@QueryParam("path") final String path) {
    return RestUtils.call(new RestUtils.RestCallable<URIStatus>() {
      @Override
      public URIStatus call() throws Exception {
        Preconditions.checkNotNull(path, "required 'path' parameter is missing");
        return mFileSystem.getStatus(new AlluxioURI(path));
      }
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
  @ReturnType("java.util.List<alluxio.client.file.URIStatus>")
  public Response listStatus(@QueryParam("path") final String path,
      @Deprecated @QueryParam("loadDirectChildren") final boolean loadDirectChildren,
      @DefaultValue("") @QueryParam("loadMetadataType") final String loadMetadataType) {
    return RestUtils.call(new RestUtils.RestCallable<List<URIStatus>>() {
      @Override
      public List<URIStatus> call() throws Exception {
        Preconditions.checkNotNull(path, "required 'path' parameter is missing");
        ListStatusOptions listStatusOptions = ListStatusOptions.defaults();
        if (!loadDirectChildren) {
          listStatusOptions.setLoadMetadataType(LoadMetadataType.Never);
        }
        // loadMetadataType overrides loadDirectChildren if it is set.
        if (!loadMetadataType.isEmpty()) {
          listStatusOptions.setLoadMetadataType(LoadMetadataType.valueOf(loadMetadataType));
        }
        return mFileSystem.listStatus(new AlluxioURI(path), listStatusOptions);
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
        MountOptions options = MountOptions.defaults();
        if (readOnly != null) {
          options.setReadOnly(readOnly);
        }
        if (shared != null) {
          options.setShared(shared);
        }
        mFileSystem.mount(new AlluxioURI(path), new AlluxioURI(ufsPath), options);
        return null;
      }
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
        mFileSystem.rename(new AlluxioURI(srcPath), new AlluxioURI(dstPath));
        return null;
      }
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
        SetAttributeOptions options = SetAttributeOptions.defaults();
        Preconditions.checkNotNull(path, "required 'path' parameter is missing");
        if (pinned != null) {
          options.setPinned(pinned);
        }
        if (ttl != null) {
          options.setTtl(ttl);
        }
        if (ttlAction != null) {
          options.setTtlAction(ttlAction);
        }
        if (persisted != null) {
          options.setPersisted(persisted);
        }
        if (owner != null) {
          options.setOwner(owner);
        }
        if (group != null) {
          options.setGroup(group);
        }
        if (permission != null) {
          options.setMode(permission);
        }
        if (recursive != null) {
          options.setRecursive(recursive);
        }
        mFileSystem.setAttribute(new AlluxioURI(path), options);
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
        mFileSystem.unmount(new AlluxioURI(path));
        return null;
      }
    });
  }

  /**
   * @summary upload a file
   * @param path the file path
   * @param persisted whether directory should be persisted
   * @param recursive whether parent directories should be created if they do not already exist
   * @param blockSizeBytes the target block size in bytes
   * @param ttl the time-to-live (in milliseconds)
   * @param ttlAction action to take after TTL is expired
   * @return the response object
   */
  @POST
  @Path(UPLOAD)
  @ReturnType("java.lang.Void")
  public Response upload(@QueryParam("path") final String path,
      @QueryParam("persisted") final Boolean persisted,
      @QueryParam("recursive") final Boolean recursive,
      @QueryParam("blockSizeBytes") final Long blockSizeBytes,
      @QueryParam("ttl") final Long ttl,
      @QueryParam("ttlAction") final TtlAction ttlAction) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        // TODO(jsimsa): Implement upload.
        // TODO(jsimsa): Support mode.
        return null;
      }
    });
  }
}
