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
<%@ page import="static java.net.URLEncoder.encode" %>

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <jsp:include page="../header-links.jsp" />
</head>
<title>Alluxio</title>
<body>
<script src="../js/jquery-1.9.1.min.js" type="text/javascript"></script>
<script src="../js/bootstrap.min.js"></script>
<% request.setAttribute("useWorkerHeader", "1"); %>
<div class="container-fluid">
  <jsp:include page="/header" />

  <div class="container-fluid">
    <div class="row-fluid">
      <div class="span12 well">
          <h1 class="text-error">
            <%= request.getAttribute("fatalError") %>
          </h1>
          <table class="table table-hover">
          <thead>
            <th>File Path</th>
            <% for (String tierAlias : (List<String>) request.getAttribute("orderedTierAliases")) { %>
            <th>In-<%= tierAlias %></th>
            <% } %>
            <th>Size</th>
            <th>Creation Time</th>
            <th>Modification Time</th>
          </thead>
          <tbody>
            <% if (request.getAttribute("fileInfos") != null) { %>
              <% for (UIFileInfo fileInfo : ((List<UIFileInfo>) request.getAttribute("fileInfos"))) { %>
                <tr>
                  <th><a href="<%= (request.getAttribute("baseUrl") == null) ? "./blockInfo" :
                  request.getAttribute("baseUrl").toString() %>?path=<%=encode(fileInfo.getAbsolutePath(), "UTF-8")%>"><%= fileInfo.getAbsolutePath() %></a></th>
                  <% for (String tierAlias : (List<String>) request.getAttribute("orderedTierAliases")) { %>
                  <th><%= fileInfo.getOnTierPercentage(tierAlias) %>%</th>
                  <% } %>
                  <th><%= fileInfo.getSize() %></th>
                  <th><%= fileInfo.getCreationTime() %></th>
                  <th><%= fileInfo.getModificationTime() %></th>
                </tr>
              <% } %>
            <% } %>
          </tbody>
        </table>

        <%@ include file="../pagination-component.jsp" %>

      </div>
    </div>
  </div>

  <%@ include file="../footer.jsp" %>
</div>

<!-- where the magic behind dynamic pagination happens -->
<%@ include file="blockInfo-pagination-header.jsp" %>
<%@ include file="../pagination-control.jsp" %>

</body>
</html>
