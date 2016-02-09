/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.master.file;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.exception.AlluxioException;
import alluxio.master.MasterContext;
import alluxio.master.AlluxioMaster;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.SetAttributeOptions;

/**
 * This class is a REST handler for file system master requests.
 */
@Path("/")
// TODO(jiri): Figure out why Jersey complains if this is changed to "/file".
public final class FileSystemMasterClientRestServiceHandler {
  public static final String SERVICE_NAME = "file/service_name";
  public static final String SERVICE_VERSION = "file/service_version";
  public static final String COMPLETE_FILE = "file/complete_file";
  public static final String CREATE_DIRECTORY = "file/create_directory";
  public static final String GET_FILE_BLOCK_INFO_LIST = "file/file_block_info_list";
  public static final String GET_STATUS = "file/status";
  public static final String GET_STATUS_INTERNAL = "file/status_internal";
  public static final String GET_UFS_ADDRESS = "file/ufs_address";
  public static final String FREE = "file/free";
  public static final String LIST_STATUS = "file/list_status";
  public static final String LOAD_METADATA = "file/load_metadata";
  public static final String MOUNT = "file/mount";
  public static final String NEW_BLOCK_ID_FOR_FILE = "file/new_block_id_for_file";
  public static final String REMOVE = "file/remove";
  public static final String RENAME = "file/rename";
  public static final String SCHEDULE_ASYNC_PERSIST = "file/schedule_async_persist";
  public static final String SET_ATTRIBUTE = "file/set_attribute";
  public static final String UNMOUNT = "file/unmount";

  private FileSystemMaster mFileSystemMaster = AlluxioMaster.get().getFileSystemMaster();

  /**
   * @return the response object
   */
  @GET
  @Path(SERVICE_NAME)
  @Produces(MediaType.APPLICATION_JSON)
  public Response name() {
    return Response.ok(Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME).build();
  }

  /**
   * @return the response object
   */
  @GET
  @Path(SERVICE_VERSION)
  @Produces(MediaType.APPLICATION_JSON)
  public Response version() {
    return Response.ok(Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION).build();
  }

  /**
   * @param path the file path
   * @param ufsLength the length of the file in under file system
   * @return the response object
   */
  @POST
  @Path(COMPLETE_FILE)
  @Produces(MediaType.APPLICATION_JSON)
  public Response completeFile(@QueryParam("path") String path,
                               @QueryParam("ufsLength") long ufsLength) {
    try {
      CompleteFileOptions options =
          new CompleteFileOptions.Builder(MasterContext.getConf()).setUfsLength(ufsLength).build();
      mFileSystemMaster.completeFile(new AlluxioURI(path), options);
      return Response.ok().build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the file path
   * @param persisted whether directory should be persisted
   * @param recursive whether parent directories should be created if they do not already exist
   * @param allowExists whether the operation should succeed even if the directory already exists
   * @return the response object
   */
  @POST
  @Path(CREATE_DIRECTORY)
  @Produces(MediaType.APPLICATION_JSON)
  public Response createDirectory(@QueryParam("path") String path,
      @QueryParam("persisted") boolean persisted, @QueryParam("recursive") boolean recursive,
      @QueryParam("allowExists") boolean allowExists) {
    try {
      CreateDirectoryOptions options =
          new CreateDirectoryOptions.Builder(MasterContext.getConf()).setAllowExists(allowExists)
              .setPersisted(persisted).setRecursive(recursive).build();
      mFileSystemMaster.mkdir(new AlluxioURI(path), options);
      return Response.ok().build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    } catch (IOException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the file path
   * @return the response object
   */
  @GET
  @Path(GET_FILE_BLOCK_INFO_LIST)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getFileBlockInfoList(@QueryParam("path") String path) {
    try {
      return Response.ok(mFileSystemMaster.getFileBlockInfoList(new AlluxioURI(path))).build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the path
   * @param recursive whether the path should be freed recursively
   * @return the response object
   */
  @POST
  @Path(FREE)
  @Produces(MediaType.APPLICATION_JSON)
  public Response free(@QueryParam("path") String path,
      @QueryParam("recursive") boolean recursive) {
    try {
      mFileSystemMaster.free(new AlluxioURI(path), recursive);
      return Response.ok().build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the file path
   * @return the response object
   */
  @GET
  @Path(LIST_STATUS)
  @Produces(MediaType.APPLICATION_JSON)
  public Response listStatus(@QueryParam("path") String path) {
    try {
      return Response.ok(mFileSystemMaster.getFileInfoList(new AlluxioURI(path))).build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param alluxioPath the alluxio path to load metadata for
   * @param recursive whether metadata should be loaded recursively
   * @return the response object
   */
  @POST
  @Path(LOAD_METADATA)
  @Produces(MediaType.APPLICATION_JSON)
  public Response loadMetadata(@QueryParam("alluxioPath") String alluxioPath,
      @QueryParam("recursive") boolean recursive) {
    try {
      return Response.ok(mFileSystemMaster.loadMetadata(new AlluxioURI(alluxioPath), recursive))
          .build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    } catch (IOException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param alluxioPath the alluxio mount point
   * @param ufsPath the UFS path to mount
   * @return the response object
   */
  @POST
  @Path(MOUNT)
  @Produces(MediaType.APPLICATION_JSON)
  public Response mount(@QueryParam("alluxioPath") String alluxioPath,
      @QueryParam("ufsPath") String ufsPath) {
    try {
      mFileSystemMaster.mount(new AlluxioURI(alluxioPath), new AlluxioURI(ufsPath));
      return Response.ok().build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    } catch (IOException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the file path
   * @return the response object
   */
  @POST
  @Path(NEW_BLOCK_ID_FOR_FILE)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getNewBlockIdForFile(@QueryParam("path") String path) {
    try {
      return Response.ok(mFileSystemMaster.getNewBlockIdForFile(new AlluxioURI(path))).build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the path to remove
   * @param recursive whether to remove paths recursively
   * @return the response object
   */
  @POST
  @Path(REMOVE)
  @Produces(MediaType.APPLICATION_JSON)
  public Response remove(@QueryParam("path") String path,
      @QueryParam("recursive") boolean recursive) {
    try {
      mFileSystemMaster.deleteFile(new AlluxioURI(path), recursive);
      return Response.ok().build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    } catch (IOException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param srcPath the source path
   * @param dstPath the destination path
   * @return the response object
   */
  @POST
  @Path(RENAME)
  @Produces(MediaType.APPLICATION_JSON)
  public Response rename(@QueryParam("srcPath") String srcPath,
      @QueryParam("dstPath") String dstPath) {
    try {
      mFileSystemMaster.rename(new AlluxioURI(srcPath), new AlluxioURI(dstPath));
      return Response.ok().build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    } catch (IOException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the file path
   * @return the response object
   */
  @GET
  @Path(SCHEDULE_ASYNC_PERSIST)
  @Produces(MediaType.APPLICATION_JSON)
  public Response scheduleAsyncPersist(@QueryParam("path") String path) {
    try {
      return Response.ok(mFileSystemMaster.scheduleAsyncPersistence(new AlluxioURI(path))).build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the file path
   * @param pinned the pinned flag value to use
   * @param ttl the time-to-live (in seconds) to use
   * @param persisted the persisted flag value to use
   * @return the response object
   */
  @PUT
  @Path(SET_ATTRIBUTE)
  @Produces(MediaType.APPLICATION_JSON)
  public Response setAttribute(@QueryParam("path") String path,
      @QueryParam("pinned") boolean pinned, @QueryParam("ttl") long ttl,
      @QueryParam("persisted") boolean persisted, @QueryParam("owner") String owner,
      @QueryParam("group") String group, @QueryParam("permission") int permission,
      @QueryParam("recursive") boolean recursive) {
    // TODO(jiri): Only set options that have been set.
    SetAttributeOptions setAttributeOptions =
        new SetAttributeOptions.Builder().setPinned(pinned).setTtl(ttl)
            .setPersisted(persisted).build();
    try {
      mFileSystemMaster.setAttribute(new AlluxioURI(path), setAttributeOptions);
      return Response.ok().build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the file path
   * @return the response object
   */
  @GET
  @Path(GET_STATUS)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getStatus(@QueryParam("path") String path) {
    try {
      return Response.ok(mFileSystemMaster.getFileInfo(new AlluxioURI(path))).build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param fileId the file id
   * @return the response path
   */
  @GET
  @Path(GET_STATUS_INTERNAL)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getStatusInternal(@QueryParam("fileId") long fileId) {
    try {
      return Response.ok(mFileSystemMaster.getFileInfo(fileId)).build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @return the response object
   */
  @GET
  @Path(GET_UFS_ADDRESS)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUfsAddress() {
    return Response.ok(mFileSystemMaster.getUfsAddress()).build();
  }

  /**
   * @param path the file path
   * @return the response object
   */
  @GET
  @Path(UNMOUNT)
  @Produces(MediaType.APPLICATION_JSON)
  public Response unmount(@QueryParam("path") String path) {
    try {
      return Response.ok(mFileSystemMaster.unmount(new AlluxioURI(path))).build();
    } catch (AlluxioException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    } catch (IOException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }
}