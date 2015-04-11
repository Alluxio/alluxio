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

package tachyon.web;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.InStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFile;
import tachyon.client.TachyonFS;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;

/**
 * Servlet for downloading a file
 */
public class WebInterfaceDownloadServlet extends HttpServlet {
  private static final long serialVersionUID = 7329267100965731815L;

  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final transient MasterInfo mMasterInfo;

  public WebInterfaceDownloadServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }

  /**
   * Prepares for downloading a file
   * 
   * @param request The HttpServletRequest object
   * @param response The HttpServletResponse object
   * @throws ServletException
   * @throws IOException
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String requestPath = request.getParameter("path");
    if (requestPath == null || requestPath.isEmpty()) {
      requestPath = TachyonURI.SEPARATOR;
    }
    boolean downloadLog = false;
    String downloadLogParam = request.getParameter("downloadLogFile");
    if (downloadLogParam != null) {
      try {
        downloadLog = Integer.valueOf(downloadLogParam) != 0;
      } catch (NumberFormatException nfe) {
        downloadLog = false;
      }
    }

    if (downloadLog) {
      // Download a log file from the local filesystem.
      TachyonConf tachyonConf = new TachyonConf();
      String baseDir = tachyonConf.get(Constants.TACHYON_HOME, Constants.DEFAULT_HOME);

      // Only allow filenames as the path, to avoid downloading any local files.
      requestPath = Paths.get(requestPath).getFileName().toString();
      Path logFilePath = Paths.get(baseDir, "/logs", "/" + requestPath);
      try {
        downloadLogFile(logFilePath, request, response);
      } catch (NoSuchFileException nsfe) {
        request.setAttribute("invalidPathError", "Error: Invalid file " + nsfe.getMessage());
        request.setAttribute("currentPath", requestPath);
        request.setAttribute("downloadLogFile", 1);
        request.setAttribute("viewingOffset", 0);
        request.setAttribute("baseUrl", "./browseLogs");
        getServletContext().getRequestDispatcher("/viewFile.jsp").forward(request, response);
      }
      return;
    }

    TachyonURI currentPath = new TachyonURI(requestPath);
    try {
      ClientFileInfo clientFileInfo = mMasterInfo.getClientFileInfo(currentPath);
      if (null == clientFileInfo) {
        throw new FileDoesNotExistException(currentPath.toString());
      }
      downloadFile(new TachyonURI(clientFileInfo.getPath()), request, response);
    } catch (FileDoesNotExistException fdne) {
      request.setAttribute("invalidPathError", "Error: Invalid Path " + fdne.getMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
    } catch (InvalidPathException ipe) {
      request.setAttribute("invalidPathError", "Error: Invalid Path " + ipe.getLocalizedMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
    }
  }

  /**
   * This function prepares for downloading a file.
   * 
   * @param path The path of the file to download
   * @param request The HttpServletRequest object
   * @param response The HttpServletResponse object
   * @throws FileDoesNotExistException
   * @throws IOException
   */
  private void downloadFile(TachyonURI path, HttpServletRequest request,
      HttpServletResponse response) throws FileDoesNotExistException, IOException {
    String masterAddress =
        Constants.HEADER + mMasterInfo.getMasterAddress().getHostName() + ":"
            + mMasterInfo.getMasterAddress().getPort();
    TachyonFS tachyonClient = TachyonFS.get(new TachyonURI(masterAddress), new TachyonConf());
    TachyonFile tFile = tachyonClient.getFile(path);
    if (tFile == null) {
      throw new FileDoesNotExistException(path.toString());
    }
    long len = tFile.length();
    String fileName = path.getName();
    response.setContentType("application/octet-stream");
    if (len <= Integer.MAX_VALUE) {
      response.setContentLength((int) len);
    } else {
      response.addHeader("Content-Length", Long.toString(len));
    }
    response.addHeader("Content-Disposition", "attachment;filename=" + fileName);

    InStream is = null;
    ServletOutputStream out = null;
    try {
      is = tFile.getInStream(ReadType.NO_CACHE);
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
    try {
      tachyonClient.close();
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
  }

  /**
   * This function prepares for downloading a log file on the local filesystem.
   *
   * @param path The path of the local log file to download
   * @param request The HttpServletRequest object
   * @param response The HttpServletResponse object
   * @throws IOException
   */
  private void downloadLogFile(Path path, HttpServletRequest request,
                               HttpServletResponse response) throws IOException {
    long len = Files.size(path);
    InputStream is = Files.newInputStream(path);
    String fileName = path.getFileName().toString();
    response.setContentType("application/octet-stream");
    if (len <= Integer.MAX_VALUE) {
      response.setContentLength((int) len);
    } else {
      response.addHeader("Content-Length", Long.toString(len));
    }
    response.addHeader("Content-Disposition", "attachment;filename=" + fileName);

    ServletOutputStream out = null;
    try {
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
