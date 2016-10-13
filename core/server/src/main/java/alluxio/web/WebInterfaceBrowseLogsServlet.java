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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet that provides data for browsing log files.
 */
@ThreadSafe
public final class WebInterfaceBrowseLogsServlet extends HttpServlet {
  private static final long serialVersionUID = 6589358568781503724L;

  private final String mBrowseJsp;
  private final String mViewJsp;
  private static final FilenameFilter LOG_FILE_FILTER = new FilenameFilter() {
    @Override
    public boolean accept(File dir, String name) {
      return name.toLowerCase().endsWith(".log");
    }
  };

  /**
   * Creates a new instance of {@link WebInterfaceBrowseLogsServlet}.
   *
   * @param isMasterServlet whether this is a master servlet
   */
  public WebInterfaceBrowseLogsServlet(boolean isMasterServlet) {
    String prefix = isMasterServlet ? "/" : "/worker/";
    mBrowseJsp = prefix + "browse.jsp";
    mViewJsp = prefix + "viewFile.jsp";
  }

  /**
   * This function displays 5KB of a file from a specific offset if it is in ASCII format.
   *
   * @param file the local file to display
   * @param request the {@link HttpServletRequest} object
   * @param offset where the file starts to display
   * @throws IOException if an I/O error occurs
   */
  private void displayLocalFile(File file, HttpServletRequest request, long offset)
      throws IOException {
    String fileData;
    try (InputStream is = new FileInputStream(file)) {
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
          fileData = WebUtils.convertByteArrayToStringWithoutEscape(data, 0, read);
        }
      }
    }
    request.setAttribute("fileData", fileData);
  }

  /**
   * Populates attribute fields with data from the MasterInfo associated with this servlet. Errors
   * will be displayed in an error field. Debugging can be enabled to display additional data. Will
   * eventually redirect the request to a jsp.
   *
   * @param request the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   * @throws IOException if the target resource throws this exception
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    request.setAttribute("debug", Configuration.getBoolean(PropertyKey.DEBUG));
    request.setAttribute("invalidPathError", "");
    request.setAttribute("viewingOffset", 0);
    request.setAttribute("downloadLogFile", 1);
    request.setAttribute("baseUrl", "./browseLogs");
    request.setAttribute("currentPath", "");
    request.setAttribute("showPermissions", false);

    String logsPath = Configuration.get(PropertyKey.LOGS_DIR);
    File logsDir = new File(logsPath);
    String requestFile = request.getParameter("path");

    if (requestFile == null || requestFile.isEmpty()) {
      // List all log files in the log/ directory.

      List<UIFileInfo> fileInfos = new ArrayList<>();
      File[] logFiles = logsDir.listFiles(LOG_FILE_FILTER);
      if (logFiles != null) {
        for (File logFile : logFiles) {
          String logFileName = logFile.getName();
          fileInfos.add(new UIFileInfo(new UIFileInfo.LocalFileInfo(logFileName, logFileName,
                  logFile.length(), UIFileInfo.LocalFileInfo.EMPTY_CREATION_TIME,
                  logFile.lastModified(), logFile.isDirectory())));
        }
      }
      Collections.sort(fileInfos, UIFileInfo.PATH_STRING_COMPARE);
      request.setAttribute("nTotalFile", fileInfos.size());

      // URL can not determine offset and limit, let javascript in jsp determine and redirect
      if (request.getParameter("offset") == null && request.getParameter("limit") == null) {
        getServletContext().getRequestDispatcher(mBrowseJsp).forward(request, response);
        return;
      }

      try {
        int offset = Integer.parseInt(request.getParameter("offset"));
        int limit = Integer.parseInt(request.getParameter("limit"));
        List<UIFileInfo> sub = fileInfos.subList(offset, offset + limit);
        request.setAttribute("fileInfos", sub);
      } catch (NumberFormatException e) {
        request.setAttribute("fatalError",
                "Error: offset or limit parse error, " + e.getLocalizedMessage());
        getServletContext().getRequestDispatcher(mBrowseJsp).forward(request, response);
        return;
      } catch (IndexOutOfBoundsException e) {
        request.setAttribute("fatalError",
                "Error: offset or offset + limit is out of bound, " + e.getLocalizedMessage());
        getServletContext().getRequestDispatcher(mBrowseJsp).forward(request, response);
        return;
      } catch (IllegalArgumentException e) {
        request.setAttribute("fatalError", e.getLocalizedMessage());
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
            relativeOffset = Long.parseLong(offsetParam);
          }
        } catch (NumberFormatException e) {
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
      } catch (IOException e) {
        request.setAttribute("invalidPathError", "Error: File " + logFile + " is not available "
                + e.getMessage());
        getServletContext().getRequestDispatcher(mViewJsp).forward(request, response);
      }
    }
  }
}
