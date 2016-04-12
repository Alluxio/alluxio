<%--
~ The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
~ (the “License”). You may not use this work except in compliance with the License, which is
~ available at www.apache.org/licenses/LICENSE-2.0
~
~ This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
~ either express or implied, as more fully set forth in the License.
~
~ See the NOTICE file distributed with this work for information regarding copyright ownership.
--%>

<%@ page import="java.util.*" %>
<%@ page import="alluxio.web.*" %>
<%@ page import="static org.apache.commons.lang.StringEscapeUtils.escapeHtml" %>
<%@ page import="static java.net.URLEncoder.encode" %>

<jsp:include page="header-scripts.jsp" />
<div class="container-fluid">
  <jsp:include page="/header" />

  <div class="container-fluid">
    <div class="row-fluid">
      <div class="span12 well">
        <h1 class="text-error">
          <%= request.getAttribute("invalidPathError") %>
        </h1>
        <div class="navbar">
          <div class="navbar-inner">
            <ul class="nav nav-pills">
              <% if (request.getAttribute("pathInfos") != null) { %>
                <% for (UIFileInfo pathInfo : ((UIFileInfo[]) request.getAttribute("pathInfos"))) { %>
                  <li><a href="./browse?path=<%= encode(pathInfo.getAbsolutePath(), "UTF-8") %>"><%= escapeHtml(pathInfo.getName()) %> </a></li>
                <% } %>
              <% } %>
              <% if (request.getAttribute("currentDirectory") != null) { %>
                <li class="active"><a href="./browse?path=<%= encode(request.getAttribute("currentPath").toString(), "UTF-8") %>"><%= escapeHtml(((UIFileInfo) request.getAttribute("currentDirectory")).getName()) %></a></li>
              <% } %>
            </ul>
          </div>
        </div>
        <table class="table table-condensed">
          <thead>
            <th>File Name</th>
            <th>Size</th>
            <th>Block Size</th>
            <th>In-Memory</th>
            <% if ((Boolean)request.getAttribute("showPermissions")) { %>
              <th>Permission</th>
              <th>Owner</th>
              <th>Group</th>
            <% } %>
            <th>Persistence State</th>
            <th>Pin</th>
            <th>Creation Time</th>
            <th>Modification Time</th>
          <!--
            <c:if test = "${debug}">
              <th>[D]Inode Number</th>
              <th>[D]Checkpoint Path</th>
            </c:if>
          -->
            <% if ((Boolean) request.getAttribute("debug")) { %>
              <th>[D]DepID</th>
              <th>[D]INumber</th>
              <th>[D]UnderfsPath</th>
              <th>[D]File Locations</th>
            <% } %>
          </thead>
          <tbody>
            <!--
            <c:forEach var="fileInfo" items="${fileInfos}">
              <tr>
                <th>
                  <c:if test = "${fileInfo.isDirectory}">
                    <i class="icon-folder-close"></i>
                  </c:if>
                  <c:if test = "${not fileInfo.isDirectory}">
                    <i class="icon-file"></i>
                  </c:if>
                  <a href="./browse?path=${fileInfo.absolutePath}"><c:out value="${fileInfo.name}"/></a>
                </th>
                <th>${fileInfo.size} Bytes</th>
                <th>${fileInfo.blockSizeBytes}</th>
                <th>
                  <c:if test = "${fileInfo.inMemory}">
                    <i class="icon-hdd"></i>
                  </c:if>
                  <c:if test = "${not fileInfo.inMemory}">
                    <i class="icon-hdd icon-white"></i>
                  </c:if>
                </th>
                <th>${fileInfo.creationTime}</th>
                <c:if test = "${debug}">
                  <th>${fileInfo.id}</th>
                  <th>${fileInfo.checkpointPath}</th>
                  <th>
                  <c:forEach var="location" items="${fileInfo.fileLocations}">
                    ${location}<br/>
                  </c:forEach>
                  </th>
                </c:if>
              </tr>
            </c:forEach>
          -->
            <% if (request.getAttribute("fileInfos") != null) { %>
              <% for (UIFileInfo fileInfo : ((List<UIFileInfo>) request.getAttribute("fileInfos"))) { %>
                <tr>
                  <th>
                    <% if (fileInfo.getIsDirectory()) { %>
                      <i class="icon-folder-close"></i>
                    <% } %>
                    <% if (!fileInfo.getIsDirectory()) { %>
                      <i class="icon-file"></i>
                    <% } %>
                    <a href="<%= (request.getAttribute("baseUrl") == null) ? "./browse" : request.getAttribute("baseUrl").toString() %>?path=<%=encode(fileInfo.getAbsolutePath(), "UTF-8")%>"><%= escapeHtml(fileInfo.getName()) %></a>
                  </th>
                  <th><%= fileInfo.getSize() %></th>
                  <th><%= fileInfo.getBlockSizeBytes() %></th>
                  <th>
                    <% if (fileInfo.getIsDirectory()) { %>
                    <% } %>
                    <% if (!fileInfo.getIsDirectory()) { %>
                      <% if (fileInfo.getInMemory()) { %>
                        <i class="icon-hdd"></i>
                      <% } %>
                      <% if (!fileInfo.getInMemory()) { %>
                        <i class="icon-hdd icon-white"></i>
                      <% } %>
                      <%= fileInfo.getInMemoryPercentage() %>%
                    <% } %>
                  </th>
                  <% if ((Boolean)request.getAttribute("showPermissions")) { %>
                    <th><%= fileInfo.getPermission() %></th>
                    <th><%= fileInfo.getUserName() %></th>
                    <th><%= fileInfo.getGroupName() %></th>
                  <% } %>
                  <th><%= (fileInfo.getPersistenceState()) %></th>
                  <th><%= (fileInfo.isPinned() ? "YES" : "NO") %></th>
                  <th><%= fileInfo.getCreationTime() %></th>
                  <th><%= fileInfo.getModificationTime() %></th>
                  <% if ((Boolean) request.getAttribute("debug")) { %>
                    <th><%= fileInfo.getId() %></th>
                    <th><% for (String location : fileInfo.getFileLocations()) { %>
                          <%= location %> <br/>
                        <% } %>
                    </th>
                  <% } %>
                </tr>
              <% } %>
            <% } %>
          </tbody>
        </table>

        <%@ include file="pagination-component.jsp" %>

      </div>
    </div>
  </div>
  <%@ include file="footer.jsp" %>
</div>

<%@ include file="browse-pagination-header.jsp" %>
<%@ include file="pagination-control.jsp" %>
