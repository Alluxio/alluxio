<%--

    The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
    (the "License"). You may not use this work except in compliance with the License, which is
    available at www.apache.org/licenses/LICENSE-2.0

    This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
    either express or implied, as more fully set forth in the License.

    See the NOTICE file distributed with this work for information regarding copyright ownership.

--%>
<!doctype html>
<%@ page import="java.util.*" %>
<%@ page import="alluxio.web.*" %>

<html ng-app="app">
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <jsp:include page="header-links.jsp" />
</head>
<title>Alluxio</title>
<body>
<jsp:include page="header-scripts.jsp" />

<script>
    var showPermissions = "<%= request.getAttribute("showPermissions")%>";
</script>
<link rel="stylesheet" href="build/node_modules/angular-ui-grid/ui-grid.min.css" type="text/css">
<script src="build/node_modules/angular-ui-grid/node_modules/angular/angular.min.js"></script>
<script src="build/node_modules/angular-ui-grid/ui-grid.min.js"></script>
<script src="js/memory.js"></script>
<div class="container-fluid">
  <jsp:include page="/header" />
  <div class="container-fluid">
    <div class="row-fluid">
      <div class="span12 well">
        <h1 class="text-error">
          <%= request.getAttribute("fatalError") %>
        </h1>

        <div id="MainCtrl" ng-controller="MainCtrl">
          <div id="grid1" ui-grid="gridOptions" ui-grid-pagination ui-grid-resize-columns ui-grid-move-columns class="grid"></div>
          <button id='toggleFiltering' ng-click="toggleFiltering()" class="btn btn-success" style="margin-top: 5px;">Toggle Filtering</button>
        </div>
      </div>
    </div>
  </div>

  <%@ include file="footer.jsp" %>
</div>
</body>
</html>
