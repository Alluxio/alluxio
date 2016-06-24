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
import alluxio.exception.AlluxioException;
import alluxio.master.AlluxioMaster;
import alluxio.master.MasterContext;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.wire.MountPointInfo;

import com.google.common.base.Preconditions;
import com.qmino.miredot.annotations.ReturnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.concurrent.NotThreadSafe;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for file system master requests.
 */
@NotThreadSafe
@Path(FileSystemMasterClientRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
public final class FileSystemMasterClientRestServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  public static final String SERVICE_PREFIX = "master/file";
  public static final String SERVICE_NAME = "service_name";
  public static final String SERVICE_VERSION = "service_version";
  public static final String COMPLETE_FILE = "complete_file";
  public static final String CREATE_DIRECTORY = "create_directory";
  public static final String CREATE_FILE = "create_file";
  public static final String FREE = "free";
  public static final String GET_FILE_BLOCK_INFO_LIST = "file_block_info_list";
  public static final String GET_MOUNT_POINTS = "mount_points";
  public static final String GET_NEW_BLOCK_ID_FOR_FILE = "new_block_id_for_file";
  public static final String GET_STATUS = "status";
  public static final String GET_STATUS_INTERNAL = "status_internal";
  public static final String GET_UFS_ADDRESS = "ufs_address";
  public static final String LIST_STATUS = "list_status";
  public static final String LOAD_METADATA = "load_metadata";
  public static final String MOUNT = "mount";
  public static final String REMOVE = "remove";
  public static final String RENAME = "rename";
  public static final String SCHEDULE_ASYNC_PERSIST = "schedule_async_persist";
  public static final String SET_ATTRIBUTE = "set_attribute";
  public static final String UNMOUNT = "unmount";

  private final FileSystemMaster mFileSystemMaster = AlluxioMaster.get().getFileSystemMaster();

  /**
   * Constructs a new {@link FileSystemMasterClientRestServiceHandler}.
   */
  public  FileSystemMasterClientRestServiceHandler() {}

  /**
   * @summary get the service name
   * @return the response object
   */
  @GET
  @Path(SERVICE_NAME)
  @ReturnType("java.lang.String")
  public Response getServiceName() {
    return RestUtils.createResponse(Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME);
  }

  /**
   * @summary get the service version
   * @return the response object
   */
  @GET
  @Path(SERVICE_VERSION)
  @ReturnType("java.lang.Long")
  public Response getServiceVersion() {
    return RestUtils.createResponse(Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_VERSION);
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
  public Response completeFile(@QueryParam("path") String path,
      @QueryParam("ufsLength") Long ufsLength) {
    try {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      CompleteFileOptions options = CompleteFileOptions.defaults();
      if (ufsLength != null) {
        options.setUfsLength(ufsLength);
      }
      mFileSystemMaster.completeFile(new AlluxioURI(path), options);
      return RestUtils.createResponse();
    } catch (AlluxioException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
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
  public Response createDirectory(@QueryParam("path") String path,
      @QueryParam("persisted") Boolean persisted, @QueryParam("recursive") Boolean recursive,
      @QueryParam("allowExists") Boolean allowExists) {
    try {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      CreateDirectoryOptions options = CreateDirectoryOptions.defaults();
      if (persisted != null) {
        options.setPersisted(persisted);
      }
      if (recursive != null) {
        options.setRecursive(recursive);
      }
      if (allowExists != null) {
        options.setAllowExists(allowExists);
      }
      mFileSystemMaster.createDirectory(new AlluxioURI(path), options);
      return RestUtils.createResponse();
    } catch (AlluxioException | IOException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary create a file
   * @param path the file path
   * @param persisted whether directory should be persisted
   * @param recursive whether parent directories should be created if they do not already exist
   * @param blockSizeBytes the target block size in bytes
   * @param ttl the time-to-live (in milliseconds)
   * @return the response object
   */
  @POST
  @Path(CREATE_FILE)
  @ReturnType("java.lang.Void")
  public Response createFile(@QueryParam("path") String path,
      @QueryParam("persisted") Boolean persisted, @QueryParam("recursive") Boolean recursive,
      @QueryParam("blockSizeBytes") Long blockSizeBytes, @QueryParam("ttl") Long ttl) {
    try {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      CreateFileOptions options = CreateFileOptions.defaults();
      if (persisted != null) {
        options.setPersisted(persisted);
      }
      if (recursive != null) {
        options.setRecursive(recursive);
      }
      if (blockSizeBytes != null) {
        options.setBlockSizeBytes(blockSizeBytes);
      }
      if (ttl != null) {
        options.setTtl(ttl);
      }
      mFileSystemMaster.createFile(new AlluxioURI(path), options);
      return RestUtils.createResponse();
    } catch (AlluxioException | IOException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary get a new block id for a file
   * @param path the file path
   * @return the response object
   */
  @POST
  @Path(GET_NEW_BLOCK_ID_FOR_FILE)
  @ReturnType("java.lang.Long")
  public Response getNewBlockIdForFile(@QueryParam("path") String path) {
    try {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      return RestUtils.createResponse(mFileSystemMaster.getNewBlockIdForFile(new AlluxioURI(path)));
    } catch (AlluxioException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }
  /**
   * @summary get a file descriptor for a path
   * @param path the file path
   * @return the response object
   */
  @GET
  @Path(GET_STATUS)
  @ReturnType("alluxio.wire.FileInfo")
  public Response getStatus(@QueryParam("path") String path) {
    try {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      return RestUtils.createResponse(mFileSystemMaster.getFileInfo(new AlluxioURI(path)));
    } catch (AlluxioException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
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
  public Response free(@QueryParam("path") String path,
      @QueryParam("recursive") boolean recursive) {
    try {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      mFileSystemMaster.free(new AlluxioURI(path), recursive);
      return RestUtils.createResponse();
    } catch (AlluxioException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary get the file descriptors for a path
   * @param path the file path
   * @param loadDirectChildren whether to load direct children of path
   * @return the response object
   */
  @GET
  @Path(LIST_STATUS)
  @ReturnType("java.util.List<alluxio.wire.FileInfo>")
  public Response listStatus(@QueryParam("path") String path,
      @QueryParam("loadDirectChildren") boolean loadDirectChildren) {
    try {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      return RestUtils.createResponse(
          mFileSystemMaster.getFileInfoList(new AlluxioURI(path), loadDirectChildren));
    } catch (AlluxioException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary mount a UFS path
   * @param path the alluxio mount point
   * @param ufsPath the UFS path to mount
   * @return the response object
   */
  @POST
  @Path(MOUNT)
  @ReturnType("java.lang.Void")
  public Response mount(@QueryParam("path") String path, @QueryParam("ufsPath") String ufsPath) {
    try {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      Preconditions.checkNotNull(ufsPath, "required 'ufsPath' parameter is missing");
      // TODO(gpang): Update the rest API to get the mount options.
      mFileSystemMaster
          .mount(new AlluxioURI(path), new AlluxioURI(ufsPath), MountOptions.defaults());
      return RestUtils.createResponse();
    } catch (AlluxioException | IOException | NullPointerException | IllegalArgumentException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary get the map from alluxio paths of mount points to the mount point details
   * @return the response object
   */
  @GET
  @Path(GET_MOUNT_POINTS)
  @ReturnType("java.util.SortedMap<java.lang.String, alluxio.wire.MountPointInfo>")
  public Response getMountPoints() {
    SortedMap<String, MountPointInfo> mountPoints = new TreeMap<>();
    for (Map.Entry<String, MountInfo> mountPoint : mFileSystemMaster.getMountTable().entrySet()) {
      MountInfo mountInfo = mountPoint.getValue();
      MountPointInfo info = new MountPointInfo();
      info.setUfsInfo(mountInfo.getUfsUri().toString(), MasterContext.getConf());
      info.setReadOnly(mountInfo.getOptions().isReadOnly());
      info.setProperties(mountInfo.getOptions().getProperties());
      mountPoints.put(mountPoint.getKey(), info);
    }
    return RestUtils.createResponse(mountPoints);
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
  public Response remove(@QueryParam("path") String path,
      @QueryParam("recursive") boolean recursive) {
    try {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      mFileSystemMaster.delete(new AlluxioURI(path), recursive);
      return RestUtils.createResponse();
    } catch (AlluxioException | IOException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
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
  public Response rename(@QueryParam("srcPath") String srcPath,
      @QueryParam("dstPath") String dstPath) {
    try {
      Preconditions.checkNotNull(srcPath, "required 'srcPath' parameter is missing");
      Preconditions.checkNotNull(dstPath, "required 'dstPath' parameter is missing");
      mFileSystemMaster.rename(new AlluxioURI(srcPath), new AlluxioURI(dstPath));
      return RestUtils.createResponse();
    } catch (AlluxioException | IOException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary schedule asynchronous persistence
   * @param path the file path
   * @return the response object
   */
  @POST
  @Path(SCHEDULE_ASYNC_PERSIST)
  @ReturnType("java.lang.Long")
  public Response scheduleAsyncPersist(@QueryParam("path") String path) {
    try {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      mFileSystemMaster.scheduleAsyncPersistence(new AlluxioURI(path));
      return RestUtils.createResponse();
    } catch (AlluxioException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
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
   * @return the response object
   */
  @POST
  @Path(SET_ATTRIBUTE)
  @ReturnType("java.lang.Void")
  public Response setAttribute(@QueryParam("path") String path,
      @QueryParam("pinned") Boolean pinned, @QueryParam("ttl") Long ttl,
      @QueryParam("persisted") Boolean persisted, @QueryParam("owner") String owner,
      @QueryParam("group") String group, @QueryParam("permission") Short permission,
      @QueryParam("recursive") Boolean recursive) {
    SetAttributeOptions options = SetAttributeOptions.defaults();
    Preconditions.checkNotNull(path, "required 'path' parameter is missing");
    if (pinned != null) {
      options.setPinned(pinned);
    }
    if (ttl != null) {
      options.setTtl(ttl);
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
    try {
      mFileSystemMaster.setAttribute(new AlluxioURI(path), options);
      return RestUtils.createResponse();
    } catch (AlluxioException | IllegalArgumentException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }

  /**
   * @summary unmount a path
   * @param path the file path
   * @return the response object
   */
  @POST
  @Path(UNMOUNT)
  @ReturnType("java.lang.Boolean")
  public Response unmount(@QueryParam("path") String path) {
    try {
      Preconditions.checkNotNull(path, "required 'path' parameter is missing");
      return RestUtils.createResponse(mFileSystemMaster.unmount(new AlluxioURI(path)));
    } catch (AlluxioException | IOException | NullPointerException e) {
      LOG.warn(e.getMessage());
      return RestUtils.createErrorResponse(e.getMessage());
    }
  }
}
