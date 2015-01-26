<%@ page import="java.util.*" %>

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
  <jsp:include page="header.jsp" />
  <div class="row-fluid">

  </div>
  <div class="row-fluid">
    <div class="accordion span14" id="accordion8">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion8" href="#data8">
            <h4>Common Configuration</h4>
          </a>
        </div>
        <div id="data8" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class = "table">
              <tbody>
                <tr>
                  <th>tachyon.home</th>
                  <th><%= request.getAttribute("tachyon.home") %></th>
                </tr>
                <tr>
                  <th>tachyon.underfs.address</th>
                  <th><%= request.getAttribute("tachyon.underfs.address") %></th>
                </tr>
                <tr>
                  <th>tachyon.data.folder</th>
                  <th><%= request.getAttribute("tachyon.data.folder") %></th>
                </tr>
                <tr>
                  <th>tachyon.workers.folder</th>
                  <th><%= request.getAttribute("tachyon.workers.folder") %></th>
                </tr>
                <tr>
                  <th>tachyon.underfs.hdfs.impl</th>
                  <th><%= request.getAttribute("tachyon.underfs.hdfs.impl") %></th>
                </tr>
                <tr>
                  <th>tachyon.underfs.glusterfs.impl</th>
                  <th><%= request.getAttribute("tachyon.underfs.glusterfs.impl") %></th>
                </tr>
                <tr>
                  <th>tachyon.underfs.glusterfs.volumes</th>
                  <th><%= request.getAttribute("tachyon.underfs.glusterfs.volumes") %></th>
                </tr>
                <tr>
                  <th>tachyon.underfs.glusterfs.mounts</th>
                  <th><%= request.getAttribute("tachyon.underfs.glusterfs.mounts") %></th>
                </tr>
                <tr>
                  <th>tachyon.underfs.glusterfs.mapred.system.dir</th>
                  <th><%= request.getAttribute("tachyon.underfs.glusterfs.mapred.system.dir") %></th>
                </tr>
                <tr>
                  <th>tachyon.web.resources</th>
                  <th><%= request.getAttribute("tachyon.web.resources") %></th>
                </tr>
                <tr>
                  <th>tachyon.usezookeeper</th>
                  <th><%= request.getAttribute("tachyon.usezookeeper") %></th>
                </tr>
                <% if(!request.getAttribute("tachyon.usezookeeper").equals("false")) { %>
                <tr>
                  <th>tachyon.zookeeper.address</th>
                  <th><%= request.getAttribute("tachyon.zookeeper.address") %></th>
                </tr>
                <tr>
                  <th>tachyon.zookeeper.election.path</th>
                  <th><%= request.getAttribute("tachyon.zookeeper.election.path") %></th>
                </tr>
                <tr>
                  <th>tachyon.zookeeper.leader.path</th>
                  <th><%= request.getAttribute("tachyon.zookeeper.leader.path") %></th>
                </tr>
                <% } %>
                <tr>
                  <th>tachyon.async.enabled</th>
                  <th><%= request.getAttribute("tachyon.async.enabled") %></th>
                </tr>
                <tr>
                  <th>tachyon.max.columns</th>
                  <th><%= request.getAttribute("tachyon.max.columns") %></th>
                </tr>
                <tr>
                  <th>tachyon.max.table.metadata.byte</th>
                  <th><%= request.getAttribute("tachyon.max.table.metadata.byte") %></th>
                </tr>
                <tr>
                  <th>tachyon.underfs.hadoop.prefixes</th>
                  <th><%= request.getAttribute("tachyon.underfs.hadoop.prefixes") %></th>
                </tr>
                <tr>
                  <th>tachyon.test.mode</th>
                  <th><%= request.getAttribute("tachyon.test.mode") %></th>
                </tr>
                <tr>
                  <th>tachyon.master.retry</th>
                  <th><%= request.getAttribute("tachyon.master.retry") %></th>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  </div>
  <div class="row-fluid">
    <div class="accordion span14" id="accordion9">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion9" href="#data9">
            <h4>Master Configuration</h4>
          </a>
        </div>
        <div id="data9" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class = "table">
              <tbody>
                <tr>
                  <th>tachyon.master.journal.folder</th>
                  <th><%= request.getAttribute("tachyon.master.journal.folder") %></th>
                </tr>
                <!--<tr>
                  <th>FORMAT_FILE_PREFIX</th>
                  <th><%= request.getAttribute("FORMAT_FILE_PREFIX") %></th>
                </tr>-->
                <tr>
                  <th>tachyon.master.hostname</th>
                  <th><%= request.getAttribute("tachyon.master.hostname") %></th>
                </tr>
                <tr>
                  <th>tachyon.master.port</th>
                  <th><%= request.getAttribute("tachyon.master.port") %></th>
                </tr>
                <!--<tr>
                  <th>MASTER_ADDRESS</th>
                  <th><%= request.getAttribute("MASTER_ADDRESS") %></th>
                </tr>-->
                <tr>
                  <th>tachyon.master.web.port</th>
                  <th><%= request.getAttribute("tachyon.master.web.port") %></th>
                </tr>
                <tr>
                  <th>tachyon.master.web.threads</th>
                  <th><%= request.getAttribute("tachyon.master.web.threads") %></th>
                </tr>
                <tr>
                  <th>tachyon.master.temporary.folder</th>
                  <th><%= request.getAttribute("tachyon.master.temporary.folder") %></th>
                </tr>
                <tr>
                  <th>tachyon.master.heartbeat.interval.ms</th>
                  <th><%= request.getAttribute("tachyon.master.heartbeat.interval.ms") %></th>
                </tr>
                <tr>
                  <th>tachyon.master.selector.threads</th>
                  <th><%= request.getAttribute("tachyon.master.selector.threads") %></th>
                </tr>
                <tr>
                  <th>tachyon.master.queue.size.per.selector</th>
                  <th><%= request.getAttribute("tachyon.master.queue.size.per.selector") %></th>
                </tr>
                <tr>
                  <th>tachyon.master.server.threads</th>
                  <th><%= request.getAttribute("tachyon.master.server.threads") %></th>
                </tr>
                <tr>
                  <th>tachyon.master.worker.timeout.ms</th>
                  <th><%= request.getAttribute("tachyon.master.worker.timeout.ms") %></th>
                </tr>
                <tr>
                  <th>tachyon.master.keytab.file</th>
                  <th><%= request.getAttribute("tachyon.master.keytab.file") %></th>
                </tr>
                <tr>
                  <th>tachyon.master.principal</th>
                  <th><%= request.getAttribute("tachyon.master.principal") %></th>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  </div>
  <div class ="row-fluid">
    <div class="accordion span14" id="accordion7">
      <div class="accordion-group">
        <div class="accordion-heading">
          <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion7" href="#data7">
            <h4>White List</h4>
          </a>
        </div>
        <div id="data7" class="accordion-body collapse in">
          <div class="accordion-inner">
            <table class = "table">
              <tbody>
                <tr>
                  <% for (String file : ((List<String>) request.getAttribute("whitelist"))) { %>
                    <tr>
                      <th><%= file %></th>
                    </tr>
                  <% } %>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  </div>
  <%@ include file="footer.jsp" %>
</div>
</body>
</html>
