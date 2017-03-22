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
import alluxio.PropertyKey;
import alluxio.web.entity.PageResultEntity;

import org.codehaus.jackson.map.ObjectMapper;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Servlet that provides ajax request for browsing the logs.
 */
@ThreadSafe
public final class WebInterfaceBrowseLogAjaxServlet extends HttpServlet {

  private static final long serialVersionUID = 7251644350146133763L;
  private static final FilenameFilter LOG_FILE_FILTER = new FilenameFilter() {
    @Override
    public boolean accept(File dir, String name) {
      return name.toLowerCase().endsWith(".log");
    }
  };

  /**
   * Creates a new instance of {@link WebInterfaceBrowseLogAjaxServlet}.
   */
  public WebInterfaceBrowseLogAjaxServlet() {
  }

  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws
      ServletException, IOException {
    doGet(req, resp);
  }

  /**
   * Populates attribute fields with data from the MasterInfo associated with this servlet. Errors
   * will be displayed in an error field. Debugging can be enabled to display additional data. Will
   * eventually redirect the request to a jsp.
   *
   * @param request  the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   * @throws IOException      if the target resource throws this exception
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    PageResultEntity pageResultEntity = new PageResultEntity();
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");

    ObjectMapper mapper = new ObjectMapper();

    String requestPath = request.getParameter("path");
    List<UIFileInfo> fileInfos = new ArrayList<>();
    pageResultEntity.getArgumentMap().put("debug", Configuration.getBoolean(PropertyKey.DEBUG));
    pageResultEntity.getArgumentMap().put("invalidPathError", "");
    pageResultEntity.getArgumentMap().put("viewingOffset", 0);
    pageResultEntity.getArgumentMap().put("downloadLogFile", 1);
    pageResultEntity.getArgumentMap().put("baseUrl", "./browseLogs");
    pageResultEntity.getArgumentMap().put("currentPath", "");
    pageResultEntity.getArgumentMap().put("showPermissions", false);

    String logsPath = Configuration.get(PropertyKey.LOGS_DIR);
    File logsDir = new File(logsPath);
    if (requestPath == null || requestPath.isEmpty()) {
      // List all log files in the log/ directory.
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
    }
    pageResultEntity.setPageData(fileInfos);
    pageResultEntity.setTotalCount(fileInfos.size());

    String json = mapper.writeValueAsString(pageResultEntity);
    response.getWriter().write(json);
  }
}
