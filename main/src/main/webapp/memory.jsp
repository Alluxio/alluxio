<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %> 

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
        <li><a href="http://tachyon-project.org/" target="_blank">Tachyon</a></li>
        <li><a href="./home">Overview</a></li>
        <li><a href="./browse?path=/">Browse File System</a></li>
        <li class="active"><a href="./memory">In Memory Files</a></li>
      </ul>
    </div>
  </div>
  
  <div class="container-fluid">
    <div class="row-fluid">
      <div class="span12 well">
        <table class="table table-hover">
          <caption>Files Currently In Memory</caption>
          <thead>
            <th>File Name</th>
            <th>Size</th>
            <th>Block Size</th>
            <th>Creation Time</th>
          </thead>
          <tbody>
            <% if (request.getAttribute("fileInfos") != null) { %>
              <% for (UiFileInfo fileInfo : ((List<UiFileInfo>) request.getAttribute("fileInfos"))) { %>
                <tr>
                  <th><%= fileInfo.getAbsolutePath() %></th>
                  <th><%= fileInfo.getSize() %></th>
                  <th><%= fileInfo.getBlockSizeBytes() %></th>
                  <th><%= fileInfo.getCreationTime() %></th>
                </tr>
              <% } %>
            <% } %>
          </tbody>
        </table>
      </div>
    </div>
  </div>
  <footer>
    <p style="text-align: center;">
      <a href="http://tachyon-project.org/">Tachyon</a> is a project developed at the UC Berkeley <a href="https://amplab.cs.berkeley.edu">AMPLab</a>.
    </p>
  </footer>
</div>
</body>
</html>
