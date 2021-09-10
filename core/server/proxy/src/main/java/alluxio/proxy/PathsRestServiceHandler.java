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
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.*;
import alluxio.web.ProxyWebServer;
import com.google.common.base.Preconditions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletContext;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is a REST handler for path resources.
 */
@NotThreadSafe
@Api(value = "/paths", description = "RESTful gateway for Alluxio Filesystem Client (Metadata)")
@Path(PathsRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public final class PathsRestServiceHandler {
    public static final String SERVICE_PREFIX = "paths";

    public static final String PATH_PARAM = "{path:.*}/";

    public static final String NANE_PARAM = "{name:.*}/";

    public static final String CREATE_DIRECTORY = "create-directory";
    public static final String CREATE_FILE      = "create-file";
    public static final String DELETE           = "delete";
    public static final String DOWNLOAD_FILE    = "download-file";
    public static final String EXISTS           = "exists";
    public static final String FREE             = "free";
    public static final String GET_STATUS       = "get-status";
    public static final String LIST_STATUS      = "list-status";
    public static final String MOUNT            = "mount";
    public static final String OPEN_FILE        = "open-file";
    public static final String RENAME           = "rename";
    public static final String SET_ATTRIBUTE    = "set-attribute";
    public static final String UNMOUNT          = "unmount";

//  private final FileSystem mFileSystem;
//  private final StreamCache mStreamCache;

    private HashMap<String, FileSystem>  mFileSystems;
    private HashMap<String, StreamCache> mStreamCaches;

    /**
     * Constructs a new {@link PathsRestServiceHandler}.
     *
     * @param context context for the servlet
     */
    public PathsRestServiceHandler(@Context ServletContext context) {
        mFileSystems  =
                (HashMap<String, FileSystem>) context.getAttribute(ProxyWebServer.FILE_SYSTEM_SERVLET_RESOURCE_POOL_KEY);
        mStreamCaches =
                (HashMap<String, StreamCache>) context.getAttribute(ProxyWebServer.STREAM_CACHE_SERVLET_RESOURCE_POOL_KEY);
//    InstancedConfiguration mSConf = ServerConfiguration.global();
//    mSConf.set(PropertyKey.SECURITY_LOGIN_USERNAME, "alice");
//    mFileSystems = FileSystem.Factory.create(mSConf);
    }

    public FileSystem getFileSystem(String name) {
        if (mFileSystems.containsKey(name)) {
            return mFileSystems.get(name);
        } else {
            InstancedConfiguration mSConf = ServerConfiguration.global();
            mSConf.set(PropertyKey.SECURITY_LOGIN_USERNAME, name);
            FileSystem mFileSystem = FileSystem.Factory.create(mSConf);
            mFileSystems.put(name, mFileSystem);
            return mFileSystem;
        }
    }

    public StreamCache getStreamCache(String name) {
        if (mStreamCaches.containsKey(name)) {
            return mStreamCaches.get(name);
        } else {
            InstancedConfiguration mSConf = ServerConfiguration.global();
            mSConf.set(PropertyKey.SECURITY_LOGIN_USERNAME, name);
            StreamCache mFileSystem = new StreamCache(ServerConfiguration.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS));
            mStreamCaches.put(name, mFileSystem);
            return mFileSystem;
        }
    }

    /**
     * @param path    the Alluxio path
     * @param options method options
     * @return the response object
     * @summary creates a directory
     */
    @POST
    @Path(PATH_PARAM + NANE_PARAM + CREATE_DIRECTORY)
    @ApiOperation(value = "Create a directory at the given path", response = java.lang.Void.class)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createDirectory(@PathParam("path") final String path, @PathParam("name") final String name,
                                    final CreateDirectoryPOptions options) {
        System.out.println(path + "_" + name + "CREATE_DIRECTORY");
        FileSystem mFileSystem = getFileSystem(name);
        return RestUtils.call((RestUtils.RestCallable<Void>) () -> {
            if (options == null) {
                mFileSystem.createDirectory(new AlluxioURI(path));
            } else {
                mFileSystem.createDirectory(new AlluxioURI(path), options);
            }
            return null;
        }, ServerConfiguration.global());
    }

    /**
     * @param path    the Alluxio path
     * @param options method options
     * @return the response object
     * @summary creates a file
     */
    @POST
    @Path(PATH_PARAM + NANE_PARAM + CREATE_FILE)
    @ApiOperation(value = "Create a file at the given path, use the id with the streams api",
            response = java.lang.Integer.class)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createFile(@PathParam("path") final String path, @PathParam("name") final String name,
                               final CreateFilePOptions options) {
        System.out.println(path + "_" + name + "CREATE_FILE");
        FileSystem  mFileSystem  = getFileSystem(name);
        StreamCache mStreamCache = getStreamCache(name);
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
     * @param path    the Alluxio path
     * @param options method options
     * @return the response object
     * @summary deletes a path
     */
    @POST
    @Path(PATH_PARAM + NANE_PARAM + DELETE)
    @ApiOperation(value = "Delete the given path", response = java.lang.Void.class)
    public Response delete(@PathParam("path") final String path, @PathParam("name") final String name, final DeletePOptions options) {
        System.out.println(path + "_" + name + "DELETE");
        FileSystem mFileSystem = getFileSystem(name);
        return RestUtils.call(new RestUtils.RestCallable<Void>() {
            @Override
            public Void call() throws Exception {
                if (options == null) {
                    mFileSystem.delete(new AlluxioURI(path));
                } else {
                    mFileSystem.delete(new AlluxioURI(path), options);
                }
                return null;
            }
        }, ServerConfiguration.global());
    }

    /**
     * @param path the path
     * @return the response
     * @summary Download a file.
     */
    @GET
    @Path(PATH_PARAM + NANE_PARAM + DOWNLOAD_FILE)
    @ApiOperation(value = "Download the given file at the path", response = java.io.InputStream.class)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response downloadFile(@PathParam("path") final String path, @PathParam("name") final String name) {
        System.out.println(path+ "_" + name + "DOWNLOAD_FILE");
        AlluxioURI          uri     = new AlluxioURI(path);
        Map<String, Object> headers = new HashMap<>();
        headers.put("Content-Disposition", "attachment; filename=" + uri.getName());
        FileSystem mFileSystem = getFileSystem(name);
        return RestUtils.call(new RestUtils.RestCallable<InputStream>() {
            @Override
            public InputStream call() throws Exception {
                FileInStream is = mFileSystem.openFile(uri);
                if (is != null) {
                    return is;
                }
                throw new IllegalArgumentException("stream does not exist");
            }
        }, ServerConfiguration.global(), headers);
    }

    /**
     * @param path    the Alluxio path
     * @param options method options
     * @return the response object
     * @summary checks whether a path exists
     */
    @POST
    @Path(PATH_PARAM + NANE_PARAM + EXISTS)
    @ApiOperation(value = "Check if the given path exists", response = java.lang.Boolean.class)
    public Response exists(@PathParam("path") final String path, @PathParam("name") final String name, final ExistsPOptions options) {
        System.out.println(path + "_" + name + "EXISTS");
        FileSystem mFileSystem = getFileSystem(name);
        return RestUtils.call(new RestUtils.RestCallable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                if (options == null) {
                    return mFileSystem.exists(new AlluxioURI(path));
                } else {
                    return mFileSystem.exists(new AlluxioURI(path), options);
                }
            }
        }, ServerConfiguration.global());
    }

    /**
     * @param path    the Alluxio path
     * @param options method options
     * @return the response object
     * @summary frees a path
     */
    @POST
    @Path(PATH_PARAM + NANE_PARAM + FREE)
    @ApiOperation(value = "Free the given path", response = java.lang.Void.class)
    public Response free(@PathParam("path") final String path, @PathParam("name") final String name, final FreePOptions options) {
        System.out.println(path + "_" + name + "FREE");
        FileSystem mFileSystem = getFileSystem(name);
        return RestUtils.call(new RestUtils.RestCallable<Void>() {
            @Override
            public Void call() throws Exception {
                if (options == null) {
                    mFileSystem.free(new AlluxioURI(path));
                } else {
                    mFileSystem.free(new AlluxioURI(path), options);
                }
                return null;
            }
        }, ServerConfiguration.global());
    }

    /**
     * @param path    the Alluxio path
     * @param options method options
     * @return the response object
     * @summary gets path status
     */
    @POST
    @Path(PATH_PARAM + NANE_PARAM + GET_STATUS)
    @ApiOperation(value = "Get the file status of the path",
            response = alluxio.client.file.URIStatus.class)
    public Response getStatus(@PathParam("path") final String path, @PathParam("name") final String name, final GetStatusPOptions options) {
        System.out.println(path + "_" + name + "GET_STATUS");
        FileSystem mFileSystem = getFileSystem(name);
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
     * @param path    the Alluxio path
     * @param options method options
     * @return the response object
     * @summary lists path statuses
     */
    @POST
    @Path(PATH_PARAM + NANE_PARAM + LIST_STATUS)
    @ApiOperation(value = "List the URIStatuses of the path's children",
            response = java.util.List.class)
    public Response listStatus(@PathParam("path") final String path, @PathParam("name") final String name,
                               final ListStatusPOptions options) {
        System.out.println(path + "_" + name + "LIST_STATUS");
        FileSystem mFileSystem = getFileSystem(name);
        return RestUtils.call(new RestUtils.RestCallable<List<URIStatus>>() {
            @Override
            public List<URIStatus> call() throws Exception {
                if (options == null) {
                    return mFileSystem
                            .listStatus(new AlluxioURI(path));
                } else {
                    return mFileSystem.listStatus(new AlluxioURI(path), options);
                }
            }
        }, ServerConfiguration.global());
    }

    /**
     * @param path    the Alluxio path
     * @param src     the UFS source to mount
     * @param options method options
     * @return the response object
     * @summary mounts a UFS path
     */
    @POST
    @Path(PATH_PARAM + NANE_PARAM + MOUNT)
    @ApiOperation(value = "Mounts the src to the given Alluxio path", response = java.lang.Void.class)
    public Response mount(@PathParam("path") final String path, @PathParam("name") final String name, @QueryParam("src") final String src,
                          final MountPOptions options) {
        FileSystem mFileSystem = getFileSystem(name);
        return RestUtils.call(new RestUtils.RestCallable<Void>() {
            @Override
            public Void call() throws Exception {
                Preconditions.checkNotNull(src, "required 'src' parameter is missing");
                if (options == null) {
                    mFileSystem.mount(new AlluxioURI(path), new AlluxioURI(src));
                } else {
                    mFileSystem.mount(new AlluxioURI(path), new AlluxioURI(src), options);
                }
                return null;
            }
        }, ServerConfiguration.global());
    }

    /**
     * @param path    the Alluxio path
     * @param options method options
     * @return the response object
     * @summary opens a file
     */
    @POST
    @Path(PATH_PARAM + NANE_PARAM + OPEN_FILE)
    @ApiOperation(value = "Opens the given path for reading, use the id with the stream api",
            response = java.lang.Integer.class)
    @Produces(MediaType.APPLICATION_JSON)
    public Response openFile(@PathParam("path") final String path, @PathParam("name") final String name, final OpenFilePOptions options) {
        FileSystem  mFileSystem  = getFileSystem(name);
        StreamCache mStreamCache = getStreamCache(name);
        return RestUtils.call(new RestUtils.RestCallable<Integer>() {
            @Override
            public Integer call() throws Exception {
                FileInStream is;
                if (options == null) {
                    is = mFileSystem.openFile(new AlluxioURI(path));
                } else {
                    is = mFileSystem.openFile(new AlluxioURI(path), options);
                }
                return mStreamCache.put(is);
            }
        }, ServerConfiguration.global());
    }

    /**
     * @param path    the Alluxio path
     * @param dst     the destination path
     * @param options method options
     * @return the response object
     * @summary renames a path
     */
    @POST
    @Path(PATH_PARAM + NANE_PARAM + RENAME)
    @ApiOperation(value = "Rename the src path to the dst path", response = java.lang.Void.class)
    public Response rename(@PathParam("path") final String path, @PathParam("name") final String name, @QueryParam("dst") final String dst,
                           final RenamePOptions options) {
        FileSystem mFileSystem = getFileSystem(name);
        return RestUtils.call(new RestUtils.RestCallable<Void>() {
            @Override
            public Void call() throws Exception {
                Preconditions.checkNotNull(dst, "required 'dst' parameter is missing");
                if (options == null) {
                    mFileSystem.rename(new AlluxioURI(path), new AlluxioURI(dst));
                } else {
                    mFileSystem.rename(new AlluxioURI(path), new AlluxioURI(dst), options);
                }
                return null;
            }
        }, ServerConfiguration.global());
    }

    /**
     * @param path    the Alluxio path
     * @param options method options
     * @return the response object
     * @summary sets an attribute
     */
    @POST
    @Path(PATH_PARAM + NANE_PARAM + SET_ATTRIBUTE)
    @ApiOperation(value = "Update attributes for the path", response = java.lang.Void.class)
    public Response setAttribute(@PathParam("path") final String path, @PathParam("name") final String name,
                                 final SetAttributePOptions options) {
        FileSystem mFileSystem = getFileSystem(name);
        return RestUtils.call(new RestUtils.RestCallable<Void>() {
            @Override
            public Void call() throws Exception {
                if (options == null) {
                    mFileSystem.setAttribute(new AlluxioURI(path));
                } else {
                    mFileSystem.setAttribute(new AlluxioURI(path), options);
                }
                return null;
            }
        }, ServerConfiguration.global());
    }

    /**
     * @param path    the Alluxio path
     * @param options method options
     * @return the response object
     * @summary unmounts a path
     */
    @POST
    @Path(PATH_PARAM + NANE_PARAM + UNMOUNT)
    @ApiOperation(value = "Unmount the path, the path must be a mount point",
            response = java.lang.Void.class)
    public Response unmount(@PathParam("path") final String path, @PathParam("name") final String name, final UnmountPOptions options) {
        FileSystem mFileSystem = getFileSystem(name);
        return RestUtils.call(new RestUtils.RestCallable<Void>() {
            @Override
            public Void call() throws Exception {
                if (options == null) {
                    mFileSystem.unmount(new AlluxioURI(path));
                } else {
                    mFileSystem.unmount(new AlluxioURI(path), options);
                }
                return null;
            }
        }, ServerConfiguration.global());
    }
}
