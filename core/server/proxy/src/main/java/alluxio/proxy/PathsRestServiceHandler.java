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
import alluxio.RestUtils;
import alluxio.StreamCache;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;
import alluxio.util.GrpcDefaultOptions;
import alluxio.web.ProxyWebServer;

import com.google.common.base.Preconditions;
import com.qmino.miredot.annotations.ReturnType;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for path resources.
 */
@NotThreadSafe
@Path(PathsRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public final class PathsRestServiceHandler {
  public static final String SERVICE_PREFIX = "paths";

  public static final String PATH_PARAM = "{path:.*}/";

  public static final String CREATE_DIRECTORY = "create-directory";
  public static final String CREATE_FILE = "create-file";
  public static final String DELETE = "delete";
  public static final String DOWNLOAD_FILE = "download-file";
  public static final String EXISTS = "exists";
  public static final String FREE = "free";
  public static final String GET_STATUS = "get-status";
  public static final String LIST_STATUS = "list-status";
  public static final String MOUNT = "mount";
  public static final String OPEN_FILE = "open-file";
  public static final String RENAME = "rename";
  public static final String SET_ATTRIBUTE = "set-attribute";
  public static final String UNMOUNT = "unmount";

  private final FileSystem mFileSystem;
  private final StreamCache mStreamCache;

  /**
   * Constructs a new {@link PathsRestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public PathsRestServiceHandler(@Context ServletContext context) {
    mFileSystem =
        (FileSystem) context.getAttribute(ProxyWebServer.FILE_SYSTEM_SERVLET_RESOURCE_KEY);
    mStreamCache =
        (StreamCache) context.getAttribute(ProxyWebServer.STREAM_CACHE_SERVLET_RESOURCE_KEY);
  }

  /**
   * @summary creates a directory
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATH_PARAM + CREATE_DIRECTORY)
  @ReturnType("java.lang.Void")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createDirectory(@PathParam("path") final String path,
      final CreateDirectoryPOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        if (options == null) {
          mFileSystem
              .createDirectory(new AlluxioURI(path),
                  GrpcDefaultOptions.getCreateDirectoryPOptions(ServerConfiguration.global()));
        } else {
          mFileSystem.createDirectory(new AlluxioURI(path), options);
        }
        return null;
      }
    }, ServerConfiguration.global());
  }

  /**
   * @summary creates a file
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATH_PARAM + CREATE_FILE)
  @ReturnType("java.lang.Integer")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createFile(@PathParam("path") final String path,
      final CreateFilePOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Integer>() {
      @Override
      public Integer call() throws Exception {
        FileOutStream os;
        if (options == null) {
          os = mFileSystem.createFile(new AlluxioURI(path));
        } else {
          os = mFileSystem.createFile(new AlluxioURI(path), options);
        }
        return mStreamCache.put(os);
      }
    }, ServerConfiguration.global());
  }

  /**
   * @summary deletes a path
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATH_PARAM + DELETE)
  @ReturnType("java.lang.Void")
  public Response delete(@PathParam("path") final String path, final DeletePOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        if (options == null) {
          mFileSystem.delete(new AlluxioURI(path),
              GrpcDefaultOptions.getDeletePOptions(ServerConfiguration.global()));
        } else {
          mFileSystem.delete(new AlluxioURI(path), options);
        }
        return null;
      }
    }, ServerConfiguration.global());
  }

  /**
   * @summary Download a file.
   * @param path the path
   * @return the response
   */
  @GET
  @Path(PATH_PARAM + DOWNLOAD_FILE)
  @ReturnType("java.io.InputStream")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response downloadFile(@PathParam("path") final String path) {
    AlluxioURI uri = new AlluxioURI(path);
    Map<String, Object> headers = new HashMap<>();
    headers.put("Content-Disposition", "attachment; filename=" + uri.getName());
    return RestUtils.call(new RestUtils.RestCallable<InputStream>() {
      @Override
      public InputStream call() throws Exception {
        FileInStream is = mFileSystem.openFile(uri,
                GrpcDefaultOptions.getOpenFilePOptions(ServerConfiguration.global()));
        if (is != null) {
          return is;
        }
        throw new IllegalArgumentException("stream does not exist");
      }
    }, ServerConfiguration.global(), headers);
  }

  /**
   * @summary checks whether a path exists
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATH_PARAM + EXISTS)
  @ReturnType("java.lang.Boolean")
  public Response exists(@PathParam("path") final String path, final ExistsPOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        if (options == null) {
          return mFileSystem.exists(new AlluxioURI(path),
              GrpcDefaultOptions.getExistsPOptions(ServerConfiguration.global()));
        } else {
          return mFileSystem.exists(new AlluxioURI(path), options);
        }
      }
    }, ServerConfiguration.global());
  }

  /**
   * @summary frees a path
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATH_PARAM + FREE)
  @ReturnType("java.lang.Void")
  public Response free(@PathParam("path") final String path, final FreePOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        if (options == null) {
          mFileSystem.free(new AlluxioURI(path),
              GrpcDefaultOptions.getFreePOptions(ServerConfiguration.global()));
        } else {
          mFileSystem.free(new AlluxioURI(path), options);
        }
        return null;
      }
    }, ServerConfiguration.global());
  }

  /**
   * @summary gets path status
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATH_PARAM + GET_STATUS)
  @ReturnType("alluxio.client.file.URIStatus")
  public Response getStatus(@PathParam("path") final String path, final GetStatusPOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<URIStatus>() {
      @Override
      public URIStatus call() throws Exception {
        if (options == null) {
          return mFileSystem.getStatus(new AlluxioURI(path));
        } else {
          return mFileSystem.getStatus(new AlluxioURI(path), options);
        }
      }
    }, ServerConfiguration.global());
  }

  /**
   * @summary lists path statuses
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATH_PARAM + LIST_STATUS)
  @ReturnType("java.util.List<alluxio.client.file.URIStatus>")
  public Response listStatus(@PathParam("path") final String path,
      final ListStatusPOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<List<URIStatus>>() {
      @Override
      public List<URIStatus> call() throws Exception {
        if (options == null) {
          return mFileSystem
              .listStatus(new AlluxioURI(path),
                  GrpcDefaultOptions.getListStatusPOptions(ServerConfiguration.global()));
        } else {
          return mFileSystem.listStatus(new AlluxioURI(path), options);
        }
      }
    }, ServerConfiguration.global());
  }

  /**
   * @summary mounts a UFS path
   * @param path the Alluxio path
   * @param src the UFS source to mount
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATH_PARAM + MOUNT)
  @ReturnType("java.lang.Void")
  public Response mount(@PathParam("path") final String path, @QueryParam("src") final String src,
      final MountPOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        Preconditions.checkNotNull(src, "required 'src' parameter is missing");
        if (options == null) {
          mFileSystem.mount(new AlluxioURI(path), new AlluxioURI(src),
                  GrpcDefaultOptions.getMountPOptions(ServerConfiguration.global()));
        } else {
          mFileSystem.mount(new AlluxioURI(path), new AlluxioURI(src), options);
        }
        return null;
      }
    }, ServerConfiguration.global());
  }

  /**
   * @summary opens a file
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATH_PARAM + OPEN_FILE)
  @ReturnType("java.lang.Integer")
  @Produces(MediaType.APPLICATION_JSON)
  public Response openFile(@PathParam("path") final String path, final OpenFilePOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Integer>() {
      @Override
      public Integer call() throws Exception {
        FileInStream is;
        if (options == null) {
          is = mFileSystem.openFile(new AlluxioURI(path),
              GrpcDefaultOptions.getOpenFilePOptions(ServerConfiguration.global()));
        } else {
          is = mFileSystem.openFile(new AlluxioURI(path), options);
        }
        return mStreamCache.put(is);
      }
    }, ServerConfiguration.global());
  }

  /**
   * @summary renames a path
   * @param path the Alluxio path
   * @param dst the destination path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATH_PARAM + RENAME)
  @ReturnType("java.lang.Void")
  public Response rename(@PathParam("path") final String path, @QueryParam("dst") final String dst,
      final RenamePOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        Preconditions.checkNotNull(dst, "required 'dst' parameter is missing");
        if (options == null) {
          mFileSystem.rename(new AlluxioURI(path), new AlluxioURI(dst),
              GrpcDefaultOptions.getRenamePOptions(ServerConfiguration.global()));
        } else {
          mFileSystem.rename(new AlluxioURI(path), new AlluxioURI(dst), options);
        }
        return null;
      }
    }, ServerConfiguration.global());
  }

  /**
   * @summary sets an attribute
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATH_PARAM + SET_ATTRIBUTE)
  @ReturnType("java.lang.Void")
  public Response setAttribute(@PathParam("path") final String path,
      final SetAttributePOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        if (options == null) {
          mFileSystem.setAttribute(new AlluxioURI(path),
              GrpcDefaultOptions.getSetAttributePOptions(ServerConfiguration.global()));
        } else {
          mFileSystem.setAttribute(new AlluxioURI(path), options);
        }
        return null;
      }
    }, ServerConfiguration.global());
  }

  /**
   * @summary unmounts a path
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATH_PARAM + UNMOUNT)
  @ReturnType("java.lang.Void")
  public Response unmount(@PathParam("path") final String path, final UnmountPOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        if (options == null) {
          mFileSystem.unmount(new AlluxioURI(path),
              GrpcDefaultOptions.getUnmountPOptions(ServerConfiguration.global()));
        } else {
          mFileSystem.unmount(new AlluxioURI(path), options);
        }
        return null;
      }
    }, ServerConfiguration.global());
  }
}
