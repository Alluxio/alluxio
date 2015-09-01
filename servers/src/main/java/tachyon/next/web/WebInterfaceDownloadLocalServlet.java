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

package tachyon.next.web;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.io.ByteStreams;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;

/**
 * Servlet for downloading a local file
 */
public final class WebInterfaceDownloadLocalServlet extends HttpServlet {
  private static final long serialVersionUID = 7260819317567193560L;

  private final transient TachyonConf mTachyonConf;

  public WebInterfaceDownloadLocalServlet() {
    mTachyonConf = new TachyonConf();
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

    // Download a file from the local filesystem.
    String baseDir = mTachyonConf.get(Constants.TACHYON_HOME);
    File logsDir = new File(baseDir, "logs");

    // Only allow filenames as the path, to avoid downloading arbitrary local files.
    requestPath = new File(requestPath).getName();
    File logFile = new File(logsDir, requestPath);
    try {
      downloadLogFile(logFile, request, response);
    } catch (FileNotFoundException fnfe) {
      request.setAttribute("invalidPathError", "Error: Invalid file " + fnfe.getMessage());
      request.setAttribute("currentPath", requestPath);
      request.setAttribute("downloadLogFile", 1);
      request.setAttribute("viewingOffset", 0);
      request.setAttribute("baseUrl", "./browseLogs");
      getServletContext().getRequestDispatcher("/viewFile.jsp").forward(request, response);
    }
  }

  /**
   * This function prepares for downloading a log file on the local filesystem.
   *
   * @param file The local log file to download
   * @param request The HttpServletRequest object
   * @param response The HttpServletResponse object
   * @throws IOException
   */
  private void downloadLogFile(File file, HttpServletRequest request,
                               HttpServletResponse response) throws IOException {
    long len = file.length();
    InputStream is = new FileInputStream(file);
    String fileName = file.getName();
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
