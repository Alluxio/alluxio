<%@ page import="java.util.*" %>
<%@ page import="tachyon.*" %>

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
      <ul class="nav nav-pills">
        <!-- <li class="active"><a href="./home">Master: ${masterNodeAddress}</a></li> -->
        <li><a href="./home">Master: <%= request.getAttribute("masterNodeAddress") %></a></li>
        <li><a href="./browse?path=/">Browse File System</a></li>
        <li><a href="./memory">View Files in Memory</a></li>
      </ul>
    </div>
  </div>
  <div class="row-fluid well">
    <% if (!((String) request.getAttribute("error")).isEmpty()) { %>
      <h1 class="text-error">
        <%= request.getAttribute("error") %>
      </h1>
    <% } %>
    <h3 class="offset4">Dependency info for <%= request.getAttribute("fileName") %>.</h3>
    <div class="span5">
      <table class="table">
        <caption>Parent Dependencies</caption>
        <tbody>
     <!-- <c:forEach var="parent" items="${parentFileNames}">
            <c:out value=${parent}/>
          </c:forEach> -->
          <% for (String parent : (List<String>) request.getAttribute("parentFileNames")) { %>
            <th><%= parent %></th>
          <% } %>
        </tbody>
      </table>
    </div>
    <div class="span5">
      <table class="table">
        <caption>Child Dependencies</caption>
        <tbody>
     <!-- <c:forEach var="child" items="${childrenFileNames}">
            <c:out value=${child}/>
          </c:forEach> -->
          <% for (String child : (List<String>) request.getAttribute("childrenFileNames")) { %>
            <th><%= child %></th>
          <% } %>
        </tbody>
      </table>
    </div>
  </div>
  <footer>
    <p style="text-align: center;">
      Tachyon is a project developed at the UC Berkeley <a href="https://amplab.cs.berkeley.edu">AMPLab</a>.
    </p>
  </footer>
</div>
</body>
</html>