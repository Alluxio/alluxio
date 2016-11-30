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
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.ExistsOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.file.options.UnmountOptions;
import alluxio.web.ProxyWebServer;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.qmino.miredot.annotations.ReturnType;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
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
@Consumes(MediaType.APPLICATION_JSON)
public final class FileSystemClientRestServiceHandler {
  public static final String SERVICE_PREFIX = "alluxio";
  public static final String PATHS_PREFIX = "paths";
  public static final String STREAMS_PREFIX = "streams";

  public static final String PATH_PARAM = "/{path:.*}/";
  public static final String ID_PARAM = "/{id}/";

  public static final String CLOSE = "close";
  public static final String CREATE_DIRECTORY = "create-directory";
  public static final String CREATE_FILE = "create-file";
  public static final String DELETE = "delete";
  public static final String EXISTS = "exists";
  public static final String FREE = "free";
  public static final String GET_STATUS = "get-status";
  public static final String LIST_STATUS = "list-status";
  public static final String MOUNT = "mount";
  public static final String OPEN_FILE = "open-file";
  public static final String READ = "read";
  public static final String RENAME = "rename";
  public static final String SET_ATTRIBUTE = "set-attribute";
  public static final String UNMOUNT = "unmount";
  public static final String WRITE = "write";

  private final FileSystem mFileSystem;
  private static Map<Integer, FileOutStream> sOutStreams = new ConcurrentHashMap<>();
  private static Map<Integer, FileInStream> sInStreams = new ConcurrentHashMap<>();

  /**
   * Constructs a new {@link FileSystemClientRestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public FileSystemClientRestServiceHandler(@Context ServletContext context) {
    mFileSystem =
        (FileSystem) context.getAttribute(ProxyWebServer.FILE_SYSTEM_SERVLET_RESOURCE_KEY);
  }

  /**
   * @summary close a stream
   * @param id the stream id
   * @return the response object
   */
  @POST
  @Path(STREAMS_PREFIX + ID_PARAM + CLOSE)
  @ReturnType("java.lang.Void")
  public Response close(@PathParam("id") final Integer id) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        FileInStream is = sInStreams.get(id);
        if (is != null) {
          is.close();
          sInStreams.remove(id);
          return null;
        }
        FileOutStream os = sOutStreams.get(id);
        if (os != null) {
          os.close();
          sOutStreams.remove(id);
          return null;
        }
        throw new IllegalArgumentException("Stream does not exist");
      }
    });
  }

  /**
   * @summary create a directory
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATHS_PREFIX + PATH_PARAM + CREATE_DIRECTORY)
  @ReturnType("java.lang.Void")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createDirectory(@PathParam("path") final String path,
      final CreateDirectoryOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        if (options == null) {
          mFileSystem.createDirectory(new AlluxioURI(path), CreateDirectoryOptions.defaults());
        } else {
          mFileSystem.createDirectory(new AlluxioURI(path), options);
        }
        return null;
      }
    });
  }

  /**
   * @summary create a file
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATHS_PREFIX + PATH_PARAM + CREATE_FILE)
  @ReturnType("java.lang.Integer")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createFile(@PathParam("path") final String path,
      final CreateFileOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Integer>() {
      @Override
      public Integer call() throws Exception {
        FileOutStream os;
        if (options == null) {
          os = mFileSystem.createFile(new AlluxioURI(path), CreateFileOptions.defaults());
        } else {
          os = mFileSystem.createFile(new AlluxioURI(path), options);
        }
        sOutStreams.put(os.hashCode(), os);
        return os.hashCode();
      }
    });
  }

  /**
   * @summary delete a path
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATHS_PREFIX + PATH_PARAM + DELETE)
  @ReturnType("java.lang.Void")
  public Response delete(@PathParam("path") final String path,
      final DeleteOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        if (options == null) {
          mFileSystem.delete(new AlluxioURI(path), DeleteOptions.defaults());
        } else {
          mFileSystem.delete(new AlluxioURI(path), options);
        }
        return null;
      }
    });
  }

  /**
   * @summary checks whether a path exists
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATHS_PREFIX + PATH_PARAM + EXISTS)
  @ReturnType("java.lang.Boolean")
  public Response exists(@PathParam("path") final String path,
      final ExistsOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        if (options == null) {
          return mFileSystem.exists(new AlluxioURI(path), ExistsOptions.defaults());
        } else {
          return mFileSystem.exists(new AlluxioURI(path), options);
        }
      }
    });
  }

  /**
   * @summary free a path
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATHS_PREFIX + PATH_PARAM + FREE)
  @ReturnType("java.lang.Void")
  public Response free(@PathParam("path") final String path,
      final FreeOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        if (options == null) {
          mFileSystem.free(new AlluxioURI(path), FreeOptions.defaults());
        } else {
          mFileSystem.free(new AlluxioURI(path), options);
        }
        return null;
      }
    });
  }

  /**
   * @summary get a file descriptor for a path
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATHS_PREFIX + PATH_PARAM + GET_STATUS)
  @ReturnType("alluxio.client.file.URIStatus")
  public Response getStatus(@PathParam("path") final String path,
      final GetStatusOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<URIStatus>() {
      @Override
      public URIStatus call() throws Exception {
        if (options == null) {
          return mFileSystem.getStatus(new AlluxioURI(path), GetStatusOptions.defaults());
        } else {
          return mFileSystem.getStatus(new AlluxioURI(path), options);
        }
      }
    });
  }

  /**
   * @summary get the file descriptors for a path
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATHS_PREFIX + PATH_PARAM + LIST_STATUS)
  @ReturnType("java.util.List<alluxio.client.file.URIStatus>")
  public Response listStatus(@PathParam("path") final String path,
      final ListStatusOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<List<URIStatus>>() {
      @Override
      public List<URIStatus> call() throws Exception {
        if (options == null) {
          return mFileSystem.listStatus(new AlluxioURI(path), ListStatusOptions.defaults());
        } else {
          return mFileSystem.listStatus(new AlluxioURI(path), options);
        }
      }
    });
  }

  /**
   * @summary mount a UFS path
   * @param path the Alluxio path
   * @param src the UFS source to mount
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATHS_PREFIX + PATH_PARAM + MOUNT)
  @ReturnType("java.lang.Void")
  public Response mount(@PathParam("path") final String path,
      @QueryParam("src") final String src,
      final MountOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        Preconditions.checkNotNull(src, "required 'src' parameter is missing");
        if (options == null) {
          mFileSystem.mount(new AlluxioURI(path), new AlluxioURI(src), MountOptions.defaults());
        } else {
          mFileSystem.mount(new AlluxioURI(path), new AlluxioURI(src), options);
        }
        return null;
      }
    });
  }

  /**
   * @summary open a file
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATHS_PREFIX + PATH_PARAM + OPEN_FILE)
  @ReturnType("java.lang.Integer")
  @Produces(MediaType.APPLICATION_JSON)
  public Response openFile(@PathParam("path") final String path,
      final OpenFileOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Integer>() {
      @Override
      public Integer call() throws Exception {
        FileInStream is;
        if (options == null) {
          is = mFileSystem.openFile(new AlluxioURI(path), OpenFileOptions.defaults());
        } else {
          is = mFileSystem.openFile(new AlluxioURI(path), options);
        }
        sInStreams.put(is.hashCode(), is);
        return is.hashCode();
      }
    });
  }

  /**
   * @summary read a stream
   * @param id the stream id
   * @return the response object
   */
  @POST
  @Path(STREAMS_PREFIX + ID_PARAM + READ)
  @ReturnType("java.lang.Void")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response read(@PathParam("id") final Integer id) {
    return RestUtils.call(new RestUtils.RestCallable<InputStream>() {
      @Override
      public InputStream call() throws Exception {
        FileInStream is = sInStreams.get(id);
        if (is != null) {
          return is;
        }
        throw new IllegalArgumentException("Stream does not exist");
      }
    });
  }

  /**
   * @summary move a path
   * @param path the Alluxio path
   * @param dst the destination path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATHS_PREFIX + PATH_PARAM + RENAME)
  @ReturnType("java.lang.Void")
  public Response rename(@PathParam("path") final String path,
      @QueryParam("dst") final String dst,
      final RenameOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        Preconditions.checkNotNull(dst, "required 'dst' parameter is missing");
        if (options == null) {
          mFileSystem.rename(new AlluxioURI(path), new AlluxioURI(dst), RenameOptions.defaults());
        } else {
          mFileSystem.rename(new AlluxioURI(path), new AlluxioURI(dst), options);
        }
        return null;
      }
    });
  }

  /**
   * @summary set an attribute
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATHS_PREFIX + PATH_PARAM + SET_ATTRIBUTE)
  @ReturnType("java.lang.Void")
  public Response setAttribute(@PathParam("path") final String path,
      final SetAttributeOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        if (options == null) {
          mFileSystem.setAttribute(new AlluxioURI(path), SetAttributeOptions.defaults());
        } else {
          mFileSystem.setAttribute(new AlluxioURI(path), options);
        }
        return null;
      }
    });
  }

  /**
   * @summary unmount a path
   * @param path the Alluxio path
   * @param options method options
   * @return the response object
   */
  @POST
  @Path(PATHS_PREFIX + PATH_PARAM + UNMOUNT)
  @ReturnType("java.lang.Void")
  public Response unmount(@PathParam("path") final String path,
      final UnmountOptions options) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        if (options == null) {
          mFileSystem.unmount(new AlluxioURI(path), UnmountOptions.defaults());
        } else {
          mFileSystem.unmount(new AlluxioURI(path), options);
        }
        return null;
      }
    });
  }

  /**
   * @summary write a stream
   * @param id the stream id
   * @param is the input stream
   * @return the response object
   */
  @POST
  @Path(STREAMS_PREFIX + ID_PARAM + WRITE)
  @ReturnType("java.lang.Void")
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Response write(@PathParam("id") final Integer id,
      final InputStream is) {
    return RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        FileOutStream os = sOutStreams.get(id);
        if (os != null) {
          ByteStreams.copy(is, os);
          return null;
        }
        throw new IllegalArgumentException("Stream does not exist");
      }
    });
  }
}
