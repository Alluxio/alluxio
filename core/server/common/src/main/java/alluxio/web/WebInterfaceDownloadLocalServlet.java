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

package alluxio.web;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;

import com.google.common.io.ByteStreams;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet for downloading a local file.
 */
@ThreadSafe
public final class WebInterfaceDownloadLocalServlet extends HttpServlet {
  private static final long serialVersionUID = 7260819317567193560L;

  /**
   * Creates a new instance of {@link WebInterfaceDownloadLocalServlet}.
   */
  public WebInterfaceDownloadLocalServlet() {}

  /**
   * Prepares for downloading a file.
   *
   * @param request the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String requestPath = request.getParameter("path");
    if (requestPath == null || requestPath.isEmpty()) {
      requestPath = AlluxioURI.SEPARATOR;
    }

    // Download a file from the local filesystem.
    File logsDir = new File(Configuration.get(PropertyKey.LOGS_DIR));

    // Only allow filenames as the path, to avoid downloading arbitrary local files.
    requestPath = new File(requestPath).getName();
    File logFile = new File(logsDir, requestPath);
    try {
      downloadLogFile(logFile, request, response);
    } catch (FileNotFoundException e) {
      request.setAttribute("invalidPathError", "Error: Invalid file " + e.getMessage());
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
   * @param file the local log file to download
   * @param request the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   */
  private void downloadLogFile(File file, HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    long len = file.length();
    String fileName = file.getName();
    response.setContentType("application/octet-stream");
    if (len <= Integer.MAX_VALUE) {
      response.setContentLength((int) len);
    } else {
      response.addHeader("Content-Length", Long.toString(len));
    }
    response.addHeader("Content-Disposition", "attachment;filename=" + fileName);

    try (InputStream is = new FileInputStream(file)) {
      ServletOutputStream out = response.getOutputStream();
      try {
        ByteStreams.copy(is, out);
      } finally {
        try {
          out.flush();
        } finally {
          out.close();
        }
      }
    }
  }
}
