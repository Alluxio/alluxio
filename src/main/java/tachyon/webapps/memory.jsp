<%@ page import="java.util.*" %>
<%@ page import="tachyon.*" %> 

<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link href="css/bootstrap.min.css" rel="stylesheet" media="screen">
</head>
<title>Tachyon</title>
<body>
<script src="js/jquery-1.9.1.min.js" type="text/javascript"></script>
<script src="js/bootstrap.min.js"></script>
<div class="container-fluid">
  <div class="navbar navbar-inverse">
    <div class="navbar-inner">
      <ul class="nav nav-pills">
        <li><a href="./home">Master: <%= request.getAttribute("masterNodeAddress") %></a></li>
        <li><a href="./browse?path=/">Browse File System</a></li>
        <li class="active"><a href="./memory">View Files in Memory</a></li>
      </ul>
    </div>
  </div>
  
  <div class="container-fluid">
    <div class="row-fluid">
      <div class="span12 well">
        <table class="table table-hover">
          <caption>Files Currently In Memory</caption>
          <tbody>
            <!--
            <c:forEach var="file" items="${inMemoryFiles}">
              <tr>
                <th>${file}</th>
              </tr>
            </c:forEach>
          -->
          <% for (String file : ((List<String>) request.getAttribute("inMemoryFiles"))) { %>
            <tr>
              <th><%= file %></th>
            </tr>
          <% } %>
          </tbody>
        </table>
      </div>
    </div>
  </div>
  <footer>
    <p>
      Tachyon is a project developed at the UC Berkeley <a href="https://amplab.cs.berkeley.edu">AMPLab</a>.
      <img src="./img/amplab_logo.png" class="offset3"/>
    </p>
  </footer>
</div>
</body>
</html>