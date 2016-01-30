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

package tachyon.master.file;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.exception.TachyonException;
import tachyon.master.MasterContext;
import tachyon.master.TachyonMaster;
import tachyon.master.file.options.CompleteFileOptions;
import tachyon.master.file.options.CreateDirectoryOptions;
import tachyon.master.file.options.SetAclOptions;
import tachyon.master.file.options.SetAttributeOptions;

/**
 * This class is a REST handler for file system master requests.
 */
@Path("/")
public class FileSystemMasterRestServiceHandler {

  /**
   * @return the response object
   */
  @GET
  @Path("file/service_name")
  @Produces(MediaType.APPLICATION_JSON)
  public Response name() {
    return Response.ok(Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME).build();
  }

  /**
   * @return the response object
   */
  @GET
  @Path("file/service_version")
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
  @Path("file/complete_file")
  @Produces(MediaType.APPLICATION_JSON)
  public Response completeFile(@QueryParam("path") String path,
      @QueryParam("ufsLength") long ufsLength) {
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    try {
      CompleteFileOptions options =
          new CompleteFileOptions.Builder(MasterContext.getConf()).setUfsLength(ufsLength).build();
      master.completeFile(new TachyonURI(path), options);
      return Response.ok().build();
    } catch (TachyonException e) {
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
  @Path("file/create_directory")
  @Produces(MediaType.APPLICATION_JSON)
  public Response createDirectory(@QueryParam("path") String path,
      @QueryParam("persisted") boolean persisted, @QueryParam("recursive") boolean recursive,
      @QueryParam("allowExists") boolean allowExists) {
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    try {
      CreateDirectoryOptions options =
          new CreateDirectoryOptions.Builder(MasterContext.getConf()).setAllowExists(allowExists)
              .setPersisted(persisted).setRecursive(recursive).build();
      master.mkdir(new TachyonURI(path), options);
      return Response.ok().build();
    } catch (TachyonException e) {
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
  @Path("file/file_block_info_list")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getFileBlockInfoList(@QueryParam("path") String path) {
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    try {
      return Response.ok(master.getFileBlockInfoList(new TachyonURI(path))).build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the path
   * @param recursive whether the path should be freed recursively
   * @return the response object
   */
  @POST
  @Path("file/free")
  @Produces(MediaType.APPLICATION_JSON)
  public Response free(@QueryParam("path") String path,
      @QueryParam("recursive") boolean recursive) {
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    try {
      master.free(new TachyonURI(path), recursive);
      return Response.ok().build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the file path
   * @return the response object
   */
  @GET
  @Path("file/list_status")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listStatus(@QueryParam("path") String path) {
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    try {
      return Response.ok(master.getFileInfoList(new TachyonURI(path))).build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param tachyonPath the tachyon path to load metadata for
   * @param recursive whether metadata should be loaded recursively
   * @return the response object
   */
  @POST
  @Path("file/load_metadata")
  @Produces(MediaType.APPLICATION_JSON)
  public Response loadMetadata(@QueryParam("tachyonPath") String tachyonPath,
                               @QueryParam("recursive") boolean recursive) {
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    try {
      return Response.ok(master.loadMetadata(new TachyonURI(tachyonPath), recursive)).build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    } catch (IOException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param tachyonPath the tachyon mount point
   * @param ufsPath the UFS path to mount
   * @return the response object
   */
  @POST
  @Path("file/mount")
  @Produces(MediaType.APPLICATION_JSON)
  public Response mount(@QueryParam("tachyonPath") String tachyonPath,
                        @QueryParam("ufsPath") String ufsPath) {
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    try {
      master.mount(new TachyonURI(tachyonPath), new TachyonURI(ufsPath));
      return Response.ok().build();
    } catch (TachyonException e) {
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
  @Path("file/new_block_id_for_file")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getNewBlockIdForFile(@QueryParam("path") String path) {
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    try {
      return Response.ok(master.getNewBlockIdForFile(new TachyonURI(path))).build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the path to remove
   * @param recursive whether to remove paths recursively
   * @return the response object
   */
  @POST
  @Path("file/remove")
  @Produces(MediaType.APPLICATION_JSON)
  public Response remove(@QueryParam("path") String path,
                         @QueryParam("recursive") boolean recursive) {
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    try {
      master.deleteFile(new TachyonURI(path), recursive);
      return Response.ok().build();
    } catch (TachyonException e) {
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
  @Path("file/rename")
  @Produces(MediaType.APPLICATION_JSON)
  public Response remove(@QueryParam("srcPath") String srcPath,
                         @QueryParam("dstPath") String dstPath) {
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    try {
      master.rename(new TachyonURI(srcPath), new TachyonURI(dstPath));
      return Response.ok().build();
    } catch (TachyonException e) {
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
  @Path("file/schedule_async_persist")
  @Produces(MediaType.APPLICATION_JSON)
  public Response scheduleAsyncPersist(@QueryParam("path") String path) {
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    try {
      return Response.ok(master.scheduleAsyncPersistence(new TachyonURI(path))).build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the file path
   * @param owner the file owner user name
   * @param group the file owner group name
   * @param permission the file permission bits
   * @param recursive whether to set permissions recursively
   * @return the response object
   */
  @PUT
  @Path("file/set_acl")
  @Produces(MediaType.APPLICATION_JSON)
  public Response setAcl(@QueryParam("path") String path, @QueryParam("owner") String owner,
      @QueryParam("group") String group, @QueryParam("permission") int permission,
      @QueryParam("recursive") boolean recursive) {
    // TODO(jiri): Only set options that have been set.
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    SetAclOptions setAclOptions =
        new SetAclOptions.Builder().setOwner(owner).setGroup(group)
            .setPermission((short) permission).setRecursive(recursive).build();
    try {
      master.setAcl(new TachyonURI(path), setAclOptions);
      return Response.ok().build();
    } catch (TachyonException e) {
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
  @Path("file/set_attribute")
  @Produces(MediaType.APPLICATION_JSON)
  public Response setAttribute(@QueryParam("path") String path,
      @QueryParam("pinned") boolean pinned, @QueryParam("ttl") long ttl,
      @QueryParam("persisted") boolean persisted) {
    // TODO(jiri): Only set options that have been set.
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    SetAttributeOptions setAttributeOptions =
        new SetAttributeOptions.Builder().setPinned(pinned).setTtl(ttl)
            .setPersisted(persisted).build();
    try {
      master.setState(new TachyonURI(path), setAttributeOptions);
      return Response.ok().build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param path the file path
   * @return the response object
   */
  @GET
  @Path("file/status")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getStatus(@QueryParam("path") String path) {
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    try {
      return Response.ok(master.getFileInfo(new TachyonURI(path))).build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @param fileId the file id
   * @return the response path
   */
  @GET
  @Path("file/status_internal")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getStatusInternal(@QueryParam("fileId") long fileId) {
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    try {
      return Response.ok(master.getFileInfo(fileId)).build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  /**
   * @return the response object
   */
  @GET
  @Path("file/ufs_address")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUfsAddress() {
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    return Response.ok(master.getUfsAddress()).build();
  }

  /**
   * @param path the file path
   * @return the response object
   */
  @GET
  @Path("file/unmount")
  @Produces(MediaType.APPLICATION_JSON)
  public Response unmount(@QueryParam("path") String path) {
    FileSystemMaster master = TachyonMaster.get().getFileSystemMaster();
    try {
      return Response.ok(master.unmount(new TachyonURI(path))).build();
    } catch (TachyonException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    } catch (IOException e) {
      return Response.serverError().status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }
}
