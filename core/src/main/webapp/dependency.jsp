<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %>

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link href="css/bootstrap.min.css" rel="stylesheet" media="screen">
  <link href="css/tachyoncustom.min.css" rel="stylesheet">
</head>
<title>Tachyon</title>
<body>
<script src="js/jquery-1.9.1.min.js" type="text/javascript"></script>
<script src="js/bootstrap.min.js"></script>
<div class="container-fluid">
  <div class="navbar navbar-inverse">
    <div class="navbar-inner">
      <ul class="nav nav-pills" style="height:40px;font-size:14px;font-weight: bold;vertical-align: bottom;">
        <li><a href="http://tachyon-project.org/" target="_blank"><img style="height:25px;margin-top:-5px;" src="img/logo.png" alt="Tachyon Logo"/></a></li>
        <li><a href="./home">Overview</a></li>
        <li><a href="./configuration">System Configuration</a></li>
        <li><a href="./browse?path=/">Browse File System</a></li>
        <li><a href="./memory">In Memory Files</a></li>
      </ul>
    </div>
  </div>
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
  <footer>
    <p style="text-align: center;">
      <a href="http://tachyon-project.org/" target="_blank">Tachyon</a> is an <a href="https://github.com/amplab/tachyon" target="_blank">open source</a> project developed at the UC Berkeley <a href="https://amplab.cs.berkeley.edu" target="_blank">AMPLab</a>.
    </p>
  </footer>
</div>
</body>
</html>
