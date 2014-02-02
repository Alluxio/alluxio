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
      <ul class="nav nav-pills">
        <li><a href="./home">Master: <%= request.getAttribute("masterNodeAddress") %></a></li>
        <li class="active"><a href="./browse?path=/">Browse File System</a></li>
        <li><a href="./memory">View Files in Memory</a></li>
      </ul>
    </div>
  </div>
  
  <div class="container-fluid">
    <div class="row-fluid">
      <div class="span12">
        <h1 class="text-error">
          <%= request.getAttribute("invalidPathError") %>
        </h1>
        <h3>The first 5KB of <%= request.getAttribute("currentPath") %> in ASCII</h3>
        <textarea class="file-content"><%= request.getAttribute("fileData") %></textarea>
      </div>
    </div>
    <hr>
    <div>
      <span>Display from position: </span>
      <input type="text" id="offset" value="<%= request.getParameter("offset") %>"></input>
      <a class="btn btn-default" onclick="displayContent();">GO!</a>
    </div>
    <hr>
    <div>
      <h3>Blocks information</h3>
      <table class="table table-bordered table-striped">
        <tr>
          <th>ID</th>
          <th>Length</th>
          <th>Status</th>
        </tr>
        <% for (WebInterfaceBrowseServlet.UiBlockInfo blockInfo : ((List<WebInterfaceBrowseServlet.UiBlockInfo>) request.getAttribute("fileBlocks"))) { %>
          <tr>
            <td><%= blockInfo.getID() %></td>
            <td><%= blockInfo.getBlockLength() %></td>
            <td>
              <% if(blockInfo.inMemory()) { %>
                in Memory
              <% } else { %>
                not in memory
              <% } %>
            </td>
          </tr>
        <% } %>
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
<script>
  function displayContent()
  {
    var tmp = document.getElementById("offset").value;
    window.location.href = "./browse?path=<%= request.getAttribute("currentPath") %>&offset=" + tmp;
  }
</script>