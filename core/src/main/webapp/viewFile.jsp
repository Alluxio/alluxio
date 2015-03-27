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
    var href = "./browse?path=<%= encode(request.getAttribute("currentPath").toString(), "UTF-8") %>&offset=" + tmp;  
    if (document.getElementById("relative_end").checked) {
      href += "&end=1";
    }
    window.location.href = href;
  }
  $(document).ready(function(){
    var download_url = "./download?path=<%= encode(request.getAttribute("currentPath").toString(), "UTF-8") %>";
    $("#file-download").attr("href",download_url);
  });
</script>
<div class="container-fluid">
  <jsp:include page="header.jsp" />

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
      <span>Display from byte offset </span>
      <input type="text" id="offset" value="<% if(request.getParameter("offset")==null) { %><%= 0 %><% } else { %><%= request.getParameter("offset") %><% } %>"></input>
      <span>  relative to </span>
      <% if(request.getParameter("end")==null) { %> 
        <input type="radio" name="rel" id="relative_begin" checked> begin </input>
        <input type="radio" name="rel" id="relative_end"> end </input>
      <% } else { %>
        <input type="radio" name="rel" id="relative_begin"> begin </input>
        <input type="radio" name="rel" id="relative_end" checked> end </input>
      <% } %>
      <a class="btn btn-default" onclick="displayContent();">GO!</a>
      <div>
        <a id="file-download">Download</a>
        <hr>
      </div>
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
  <%@ include file="footer.jsp" %>
</div>
</body>
</html>
