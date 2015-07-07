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

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

/**
 * Servlet that provides data for browsing log files.
 */
public class WebInterfaceBrowseLogsServlet extends HttpServlet {
  private static final long serialVersionUID = 6589358568781503724L;

  private final transient TachyonConf mTachyonConf;
  private final String mBrowseJsp;
  private final String mViewJsp;
  private final FilenameFilter mLogFileFilter;

  public WebInterfaceBrowseLogsServlet(boolean isMasterServlet) {
    mTachyonConf = new TachyonConf();
    String prefix = isMasterServlet ? "/" : "/worker/";
    mBrowseJsp = prefix + "browse.jsp";
    mViewJsp = prefix + "viewFile.jsp";
    mLogFileFilter = new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.toLowerCase().endsWith(".log");
      }
    };
  }

  /**
   * This function displays 5KB of a file from a specific offset if it is in ASCII format.
   *
   * @param file The local file to display
   * @param request The HttpServletRequest object
   * @param offset Where the file starts to display.
   * @throws IOException
   */
  private void displayLocalFile(File file, HttpServletRequest request, long offset)
      throws IOException {
    String fileData = null;
    InputStream is = new FileInputStream(file);
    try {
      long fileSize = file.length();
      int len = (int) Math.min(5 * Constants.KB, fileSize - offset);
      byte[] data = new byte[len];
      long skipped = is.skip(offset);
      if (skipped < 0) {
        // nothing was skipped
        fileData = "Unable to traverse to offset; is file empty?";
      } else if (skipped < offset) {
        // couldn't skip all the way to offset
        fileData = "Unable to traverse to offset; is offset larger than the file?";
      } else {
        // read may not read up to len, so only convert what was read
        int read = is.read(data, 0, len);
        if (read < 0) {
          // stream couldn't read anything, skip went to EOF?
          fileData = "Unable to read file";
        } else {
          fileData = Utils.convertByteArrayToStringWithoutEscape(data, 0, read);
        }
      }
    } finally {
      is.close();
    }
    request.setAttribute("fileData", fileData);
  }

  /**
   * Populates attribute fields with data from the MasterInfo associated with this servlet. Errors
   * will be displayed in an error field. Debugging can be enabled to display additional data. Will
   * eventually redirect the request to a jsp.
   *
   * @param request The HttpServletRequest object
   * @param response The HttpServletResponse object
   * @throws ServletException
   * @throws IOException
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    request.setAttribute("debug", Constants.DEBUG);
    request.setAttribute("invalidPathError", "");
    request.setAttribute("viewingOffset", 0);
    request.setAttribute("downloadLogFile", 1);
    request.setAttribute("baseUrl", "./browseLogs");
    request.setAttribute("currentPath", "");

    String tachyonHome = mTachyonConf.get(Constants.TACHYON_HOME, Constants.DEFAULT_HOME);
    String logsPath =
        mTachyonConf.get(Constants.LOGS_DIR, CommonUtils.concatPath(tachyonHome, "logs"));
    File logsDir = new File(logsPath);
    String requestFile = request.getParameter("path");

    if (requestFile == null || requestFile.isEmpty()) {
      // List all log files in the log/ directory.

      List<UiFileInfo> fileInfos = new ArrayList<UiFileInfo>();
      File[] logFiles = logsDir.listFiles(mLogFileFilter);
      for (File logFile : logFiles) {
        String logFileName = logFile.getName();
        fileInfos.add(new UiFileInfo(new UiFileInfo.LocalFileInfo(logFileName, logFileName,
                logFile.length(), UiFileInfo.LocalFileInfo.EMPTY_CREATION_TIME,
                logFile.lastModified(), logFile.isDirectory())));
      }
      Collections.sort(fileInfos, UiFileInfo.PATH_STRING_COMPARE);
      request.setAttribute("nTotalFile", fileInfos.size());

      // URL can not determine offset and limit, let javascript in jsp determine and redirect
      if (request.getParameter("offset") == null && request.getParameter("limit") == null) {
        getServletContext().getRequestDispatcher(mBrowseJsp).forward(request, response);
        return;
      }

      try {
        int offset = Integer.parseInt(request.getParameter("offset"));
        int limit = Integer.parseInt(request.getParameter("limit"));
        List<UiFileInfo> sub = fileInfos.subList(offset, offset + limit);
        request.setAttribute("fileInfos", sub);
      } catch (NumberFormatException nfe) {
        request.setAttribute("fatalError",
                "Error: offset or limit parse error, " + nfe.getLocalizedMessage());
        getServletContext().getRequestDispatcher(mBrowseJsp).forward(request, response);
        return;
      } catch (IndexOutOfBoundsException iobe) {
        request.setAttribute("fatalError",
                "Error: offset or offset + limit is out of bound, " + iobe.getLocalizedMessage());
        getServletContext().getRequestDispatcher(mBrowseJsp).forward(request, response);
        return;
      } catch (IllegalArgumentException iae) {
        request.setAttribute("fatalError", iae.getLocalizedMessage());
        getServletContext().getRequestDispatcher(mBrowseJsp).forward(request, response);
        return;
      }

      getServletContext().getRequestDispatcher(mBrowseJsp).forward(request, response);
    } else {
      // Request a specific log file.

      // Only allow filenames as the path, to avoid arbitrary local path lookups.
      requestFile = new File(requestFile).getName();
      request.setAttribute("currentPath", requestFile);

      File logFile = new File(logsDir, requestFile);

      try {
        long fileSize = logFile.length();
        String offsetParam = request.getParameter("offset");
        long relativeOffset = 0;
        long offset;
        try {
          if (offsetParam != null) {
            relativeOffset = Long.valueOf(offsetParam);
          }
        } catch (NumberFormatException nfe) {
          relativeOffset = 0;
        }
        String endParam = request.getParameter("end");
        // If no param "end" presents, the offset is relative to the beginning; otherwise, it is
        // relative to the end of the file.
        if (endParam == null) {
          offset = relativeOffset;
        } else {
          offset = fileSize - relativeOffset;
        }
        if (offset < 0) {
          offset = 0;
        } else if (offset > fileSize) {
          offset = fileSize;
        }

        displayLocalFile(logFile, request, offset);
        request.setAttribute("viewingOffset", offset);
        getServletContext().getRequestDispatcher(mViewJsp).forward(request, response);
      } catch (IOException ie) {
        request.setAttribute("invalidPathError", "Error: File " + logFile + " is not available "
                + ie.getMessage());
        getServletContext().getRequestDispatcher(mViewJsp).forward(request, response);
      }
    }
  }
}
