<div class="navbar">
  <div class="navbar-inner">
    <ul class="nav nav-pills" style="height:40px;font-size:14px;font-weight:bold;vertical-align:bottom;">
      <li><a href="http://alluxio.org/" target="_blank"><img style="height:25px;margin-top:-5px;" src="img/logo.png" alt="Alluxio Logo"/></a></li>
      <li id="home-li"><a href="./home">Overview</a></li>
      <% if (request.getAttribute("useWorkerHeader") == null) { %>
        <li id="browse-li"><a href="./browse?path=/">Browse File System</a></li>
        <li id="configuration-li"><a href="./configuration">System Configuration</a></li>
        <li id="workers-li"><a href="./workers">Workers</a></li>
        <li id="memory-li"><a href="./memory">In-Memory Files</a></li>
      <% } else {%>
        <li id="blockInfo-li"><a href="./blockInfo">BlockInfo</a></li>
      <% } %>
      <li id="browseLogs-li"><a href="./browseLogs">Log Files</a></li>
      <li id="autorefresh-li"><a href="javascript:toggleAutoRefresh();" id="autorefresh-link">Enable Auto-Refresh</a></li>
      <li id="metrics-li"><a href="./metrics">Metrics</a></li>
      <% if (request.getAttribute("useWorkerHeader") != null) { %>
        <li id="returnmaster-li"><a href="http://<%=request.getAttribute("masterHost") %>:<%= request.getAttribute("masterPort") %>">Return to Master</a></li>
      <% } %>
    </ul>
  </div>
</div>
<script>
  var url = window.location.pathname;
  $("#"+url.substring(1)+"-li").addClass("active");
</script>
