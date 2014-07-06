<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %>
<%@ page import="static org.apache.commons.lang.StringEscapeUtils.escapeHtml" %>
<%@ page import="static java.net.URLEncoder.encode" %>

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
<script>
  function displayContent()
  {
    var tmp = document.getElementById("offset").value;
    window.location.href = "./browse?path=<%= encode(request.getAttribute("currentPath").toString(), "UTF-8") %>&offset=" + tmp;
  }
</script>
<div class="container-fluid">
  <div class="navbar navbar-inverse">
    <div class="navbar-inner">
      <ul class="nav nav-pills" style="height:40px;font-size:14px;font-weight: bold;vertical-align: bottom;">
        <li><a href="http://tachyon-project.org/" target="_blank"><img style="height:25px;margin-top:-5px;" src="img/logo.png" alt="Tachyon Logo"/></a></li>
        <li><a href="./home">Overview</a></li>
        <li><a href="./configuration">System Configuration</a></li>
        <li class="active"><a href="./browse?path=/">Browse File System</a></li>
        <li><a href="./memory">In Memory Files</a></li>
      </ul>
    </div>
  </div>

  <div class="container-fluid">
    <div class="row-fluid">
      <div class="span12">
        <h1 class="text-error">
          <%= request.getAttribute("invalidPathError") %>
        </h1>
        <h4><%= escapeHtml(request.getAttribute("currentPath").toString()) %>: First 5KB from <%= request.getAttribute("viewingOffset") %> in ASCII</h4>
        <textarea class="file-content"><%= request.getAttribute("fileData") %></textarea>
      </div>
    </div>
    <hr>
    <div>
      <span>Display from position: </span>
      <input type="text" id="offset" value="<% if(request.getParameter("offset")==null) { %><%= 0 %><% } else { %><%= request.getParameter("offset") %><% } %>"></input>
      <a class="btn btn-default" onclick="displayContent();">GO!</a>
    </div>
    <hr>
    <div>
      <h5>Detailed blocks information (block capacity is <%= request.getAttribute("blockSizeByte") %> Bytes):</h5>
      <table class="table table-bordered table-striped">
        <tr>
          <th>ID</th>
          <th>Size (Byte)</th>
          <th>In Memory</th>
        </tr>
        <% for (WebInterfaceBrowseServlet.UiBlockInfo blockInfo : ((List<WebInterfaceBrowseServlet.UiBlockInfo>) request.getAttribute("fileBlocks"))) { %>
          <tr>
            <td><%= blockInfo.getID() %></td>
            <td><%= blockInfo.getBlockLength() %></td>
            <td>
              <% if(blockInfo.inMemory()) { %>
                Yes
              <% } else { %>
                No
              <% } %>
            </td>
          </tr>
        <% } %>
      </table>
    </div>
  </div>
  <hr>
  <footer>
    <p style="text-align: center;">
      <a href="http://tachyon-project.org/" target="_blank">Tachyon</a> is an <a href="https://github.com/amplab/tachyon" target="_blank">open source</a> project developed at the UC Berkeley <a href="https://amplab.cs.berkeley.edu" target="_blank">AMPLab</a>.
    </p>
  </footer>
</div>
</body>
</html>
