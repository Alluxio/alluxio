<%--

    The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
    (the "License"). You may not use this work except in compliance with the License, which is
    available at www.apache.org/licenses/LICENSE-2.0

    This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
    either express or implied, as more fully set forth in the License.

    See the NOTICE file distributed with this work for information regarding copyright ownership.

--%>
<%@ page import="java.util.*" %>
<%@ page import="alluxio.web.*" %>
<%@ page import="static org.apache.commons.lang.StringEscapeUtils.escapeHtml" %>
<%@ page import="static java.net.URLEncoder.encode" %>

<jsp:include page="header-scripts.jsp" />

<link rel="stylesheet" href="build/node_modules/angular-ui-grid/ui-grid.min.css" type="text/css">
<script src="build/node_modules/angular-ui-grid/node_modules/angular/angular.min.js"></script>
<script src="build/node_modules/angular-ui-grid/ui-grid.min.js"></script>
<script src="js/browse.js" ></script>

<script>
    var currentDir = "<%= request.getAttribute("currentPath").toString()%>";
    var base = "<%= (request.getAttribute("baseUrl") == null) ? "./browse" : request.getAttribute("baseUrl").toString() %>";
    var showPermissions = "<%= request.getAttribute("showPermissions")%>";
</script>

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
            <ul id="pathUl" class="nav nav-pills">
            </ul>
            <div id="pathNav" class="input-append pull-right" style="margin: 5px; margin-right: 45px;width:600px;">
              <input class="span12" id="pathInput" type="text" placeholder="Navigate to a directory">
              <button class="btn" id="goBtn" type="button">Go</button>
            </div>
          </div>
        </div>
        <div ng-app="app" id="MainCtrl" ng-controller="MainCtrl">
          <div id="grid1" ui-grid="gridOptions" ui-grid-pagination ui-grid-resize-columns ui-grid-move-columns class="grid"></div>
            <button id='toggleFiltering' ng-click="toggleFiltering()" class="btn btn-success" style="margin-top: 5px;">Toggle Filtering</button>
          </div>
        </div>
    </div>
  </div>
  <%@ include file="footer.jsp" %>
</div>
