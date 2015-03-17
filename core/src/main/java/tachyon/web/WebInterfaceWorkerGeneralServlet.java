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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import tachyon.Constants;
import tachyon.StorageLevelAlias;
import tachyon.Version;
import tachyon.util.CommonUtils;
import tachyon.worker.WorkerStorage;

public class WebInterfaceWorkerGeneralServlet extends HttpServlet {
  private static final long serialVersionUID = 3735143768058466487L;
  private final transient WorkerStorage mWorkerStorage;

  public WebInterfaceWorkerGeneralServlet(WorkerStorage workerStorage) {
    mWorkerStorage = workerStorage;
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
          throws ServletException, IOException {
    populateValues(request);
    getServletContext().getRequestDispatcher("/worker/general.jsp").forward(request, response);
  }

  /**
   * Populates key, value pairs for UI display
   *
   * @param request The HttpServletRequest object
   * @throws IOException
   */
  private void populateValues(HttpServletRequest request) throws IOException {
    request.setAttribute("debug", Constants.DEBUG);

    request.setAttribute("workerAddress", mWorkerStorage.getWorkerAddress().toString());

    request.setAttribute(
            "uptime",
            CommonUtils.convertMsToClockTime(System.currentTimeMillis()
                    - mWorkerStorage.getStarttimeMs()));

    request
        .setAttribute("startTime", CommonUtils.convertMsToDate(mWorkerStorage.getStarttimeMs()));

    request.setAttribute("version", Version.VERSION);

    request.setAttribute("capacityBytes",
        CommonUtils.getSizeFromBytes(mWorkerStorage.getCapacityBytes()));

    request.setAttribute("usedBytes",
            CommonUtils.getSizeFromBytes(mWorkerStorage.getUsedBytes()));

    request.setAttribute("memCapacityBytes",
        CommonUtils.getSizeFromBytes(mWorkerStorage.getCapacityBytes(StorageLevelAlias.MEM)));

    request.setAttribute("memUsedBytes",
        CommonUtils.getSizeFromBytes(mWorkerStorage.getUsedBytes(StorageLevelAlias.MEM)));

    request.setAttribute("ssdCapacityBytes",
        CommonUtils.getSizeFromBytes(mWorkerStorage.getCapacityBytes(StorageLevelAlias.SSD)));

    request.setAttribute("ssdUsedBytes",
        CommonUtils.getSizeFromBytes(mWorkerStorage.getUsedBytes(StorageLevelAlias.SSD)));

    request.setAttribute("hddCapacityBytes",
        CommonUtils.getSizeFromBytes(mWorkerStorage.getCapacityBytes(StorageLevelAlias.HDD)));

    request.setAttribute("hddUsedBytes",
        CommonUtils.getSizeFromBytes(mWorkerStorage.getUsedBytes(StorageLevelAlias.HDD)));

    request.setAttribute("storageDirs", mWorkerStorage.getStorageDirs());
  }
}
