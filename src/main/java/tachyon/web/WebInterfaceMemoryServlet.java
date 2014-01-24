/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.web;

import java.util.List;
import java.util.Collections;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import tachyon.MasterInfo;

import java.io.IOException;

/**
 * Servlet that provides data for displaying which files are currently in memory.
 */
public class WebInterfaceMemoryServlet extends HttpServlet {
  private static final long serialVersionUID = 4293149962399443914L;
  private MasterInfo mMasterInfo;

  public WebInterfaceMemoryServlet(MasterInfo masterInfo) {
    mMasterInfo = masterInfo;
  }
  
  /**
   * Populates attributes before redirecting to a jsp.
   * @param request The HttpServletRequest object
   * @param response The HttpServletReponse object
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) 
      throws ServletException, IOException {
    request.setAttribute("masterNodeAddress", mMasterInfo.getMasterAddress().toString());
    List<String> inMemoryFiles = mMasterInfo.getInMemoryFiles();
    Collections.sort(inMemoryFiles);
    request.setAttribute("inMemoryFiles", inMemoryFiles);
    getServletContext().getRequestDispatcher("/memory.jsp").forward(request, response);
  }
}