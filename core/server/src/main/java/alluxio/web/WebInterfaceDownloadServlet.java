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

package alluxio.web;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.AlluxioException;
import alluxio.master.MasterContext;
import alluxio.master.file.FileSystemMaster;
import alluxio.security.LoginUser;
import alluxio.security.authentication.PlainSaslServer;
import alluxio.util.SecurityUtils;
import alluxio.wire.FileInfo;

/**
 * Servlet for downloading a file.
 */
@ThreadSafe
public final class WebInterfaceDownloadServlet extends HttpServlet {
  private static final long serialVersionUID = 7329267100965731815L;

  private final transient FileSystemMaster mFsMaster;

  /**
   * Creates a new instance of {@link WebInterfaceDownloadServlet}.
   *
   * @param fsMaster file system master
   */
  public WebInterfaceDownloadServlet(FileSystemMaster fsMaster) {
    mFsMaster = Preconditions.checkNotNull(fsMaster);
  }

  /**
   * Prepares for downloading a file.
   *
   * @param request the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   * @throws IOException if the target resource throws this exception
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    if (SecurityUtils.isSecurityEnabled(MasterContext.getConf())
        && PlainSaslServer.AuthorizedClientUser.get(MasterContext.getConf()) == null) {
      PlainSaslServer.AuthorizedClientUser.set(LoginUser.get(MasterContext.getConf()).getName());
    }
    String requestPath = request.getParameter("path");
    if (requestPath == null || requestPath.isEmpty()) {
      requestPath = AlluxioURI.SEPARATOR;
    }
    AlluxioURI currentPath = new AlluxioURI(requestPath);
    try {
      long fileId = mFsMaster.getFileId(currentPath);
      FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
      if (fileInfo == null) {
        throw new FileDoesNotExistException(currentPath.toString());
      }
      downloadFile(new AlluxioURI(fileInfo.getPath()), request, response);
    } catch (FileDoesNotExistException e) {
      request.setAttribute("invalidPathError", "Error: Invalid Path " + e.getMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
    } catch (InvalidPathException e) {
      request.setAttribute("invalidPathError", "Error: Invalid Path " + e.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
    } catch (AlluxioException e) {
      request.setAttribute("invalidPathError", "Error: " + e.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
    }
  }

  /**
   * This function prepares for downloading a file.
   *
   * @param path the path of the file to download
   * @param request the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   */
  private void downloadFile(AlluxioURI path, HttpServletRequest request,
      HttpServletResponse response) throws FileDoesNotExistException, IOException,
      InvalidPathException, AlluxioException {
    FileSystem alluxioClient = FileSystem.Factory.get();
    URIStatus status = alluxioClient.getStatus(path);
    long len = status.getLength();
    String fileName = path.getName();
    response.setContentType("application/octet-stream");
    if (len <= Integer.MAX_VALUE) {
      response.setContentLength((int) len);
    } else {
      response.addHeader("Content-Length", Long.toString(len));
    }
    response.addHeader("Content-Disposition", "attachment;filename=" + fileName);

    FileInStream is = null;
    ServletOutputStream out = null;
    try {
      // TODO(jiri): Should we use MasterContext here instead?
      OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
      is = alluxioClient.openFile(path, options);
      out = response.getOutputStream();
      ByteStreams.copy(is, out);
    } finally {
      if (out != null) {
        out.flush();
        out.close();
      }
      if (is != null) {
        is.close();
      }
    }
  }
}
