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

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <jsp:include page="header-links.jsp" />
</head>
<title>Alluxio</title>
<body>
<jsp:include page="header-scripts.jsp" />
<div class="container-fluid">
  <jsp:include page="/header" />
  <div class="row-fluid">
    <% if (!((String) request.getAttribute("error")).isEmpty()) { %>
      <h1 class="text-error">
        <%= request.getAttribute("error") %>
      </h1>
    <% } %>
    <h3 class="offset2">Dependency info for <%= request.getAttribute("filePath") %>.</h3>
    <div class="well span5">
      <table class="table">
        <caption>Parent Files</caption>
        <tbody>
     <!-- <c:forEach var="parent" items="${parentFileNames}">
            <tr><th><c:out value=${parent}/></th></tr>
          </c:forEach> -->
          <% for (String parent : (List<String>) request.getAttribute("parentFileNames")) { %>
            <tr><th><%= parent %></th></tr>
          <% } %>
        </tbody>
      </table>
    </div>
    <div class="well offset1 span5">
      <table class="table">
        <caption>Children Files</caption>
        <tbody>
     <!-- <c:forEach var="child" items="${childrenFileNames}">
            <tr><th><c:out value=${child}/></th></tr>
          </c:forEach> -->
          <% for (String child : (List<String>) request.getAttribute("childrenFileNames")) { %>
            <tr><th><%= child %></th></tr>
          <% } %>
        </tbody>
      </table>
    </div>
  </div>
  <%@ include file="footer.jsp" %>
</div>
</body>
</html>
