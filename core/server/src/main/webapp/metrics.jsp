<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %>
<%@ page import="tachyon.web.*" %>

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link href="css/bootstrap.min.css" rel="stylesheet" media="screen">
  <link href="css/tachyoncustom.min.css" rel="stylesheet">
  <link href="css/linearGraph.css" rel="stylesheet">
</head>
<title>Tachyon</title>
<body>
<jsp:include page="header-scripts.jsp" />
<script src="js/d3.min.js" type="text/javascript"></script>
<script src="js/linearGraph.js" type="text/javascript"></script>
<div class="container-fluid">
  <jsp:include page="/header" />

    <div class="row-fluid">
      <div class="accordion span14" id="accordion3">
        <div class="accordion-group">
          <div class="accordion-heading">
            <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion3" href="#data3">
              <h4>Master gauges</h4>
            </a>
          </div>
          
          <div id="data3" class="accordion-body collapse in">
            <div class="accordion-inner">
              <table class="table table-hover table-condensed">
                <thead>                  
                  <th></th>                  
                  <th></th>
                </thead>
                <tbody>
                  <tr>                    
                    <th>Master Capacity</th>                    
                    <th>
                      <div class="progress custom-progress">
                        <div class="bar bar-success" style="width: <%= (Integer) request.getAttribute("masterCapacityFreePercentage") %>%;">
                          <% if ( (Integer) request.getAttribute("masterCapacityFreePercentage") >= (Integer) request.getAttribute("masterCapacityUsedPercentage")) { %>
                            <%= request.getAttribute("masterCapacityFreePercentage") %>%Free
                          <% } %>
                        </div>
                        <div class="bar bar-danger" style="width: <%= (Integer) request.getAttribute("masterCapacityUsedPercentage") %>%;">
                          <% if ((Integer) request.getAttribute("masterCapacityFreePercentage") < (Integer) request.getAttribute("masterCapacityUsedPercentage")) { %>
                            <%= request.getAttribute("masterCapacityUsedPercentage") %>%Used
                          <% } %>
                        </div>
                      </div>
                    </th>
                  </tr>
                  
                  <tr>                    
                    <th>Master Underfs Capacity</th>                    
                    <th>
                      <div class="progress custom-progress">
                        <div class="bar bar-success" style="width: <%= (Integer) request.getAttribute("masterUnderfsCapacityFreePercentage") %>%;">
                          <% if ( (Integer) request.getAttribute("masterUnderfsCapacityFreePercentage") >= (Integer) request.getAttribute("masterUnderfsCapacityUsedPercentage")) { %>
                            <%= request.getAttribute("masterCapacityFreePercentage") %>%Free
                          <% } %>
                        </div>
                        <div class="bar bar-danger" style="width: <%= (Integer) request.getAttribute("masterUnderfsCapacityUsedPercentage") %>%;">
                          <% if ((Integer) request.getAttribute("masterUnderfsCapacityFreePercentage") < (Integer) request.getAttribute("masterUnderfsCapacityUsedPercentage")) { %>
                            <%= request.getAttribute("masterUnderfsCapacityUsedPercentage") %>%Used
                          <% } %>
                        </div>
                      </div>
                    </th>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
          
        </div>
      </div>
    </div>
    
    <div class="row-fluid">
      
      <div class="accordion span14" id="accordion4">
        <div class="accordion-group">
          <div class="accordion-heading">
            <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion4" href="#data4">
              <h4>Operations</h4>
            </a>
          </div>
          
          <div id="data4" class="accordion-body collapse in">
            <div class="accordion-inner">
              
              <table class="table table-hover table-condensed">
                <thead>                  
                  <th></th>                  
                  <th></th>
                </thead>
                <tbody>
                  <tr>                    
                    <th>Directories Created</th>
                    <th>File Block Infos Got</th>
                  </tr>
                  <tr>
                    <th>
                      <div id="DirectoriesCreated">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#DirectoriesCreated", jsonString["master.DirectoriesCreated"]);
                        </script>
                      </div>
                      
                    </th>
                    <th>
                      <div id="FileBlockInfosGot">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#FileBlockInfosGot", jsonString["master.FileBlockInfosGot"]);
                        </script>
                      </div>
                    </th>
                  </tr>
                  <tr>                    
                    <th>File Infos Got</th>
                    <th>Files Completed</th>
                  </tr>
                  <tr>
                    <th>
                      <div id="FileInfosGot">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#FileInfosGot", jsonString["master.FileInfosGot"]);
                        </script>
                      </div>
                      
                    </th>
                    <th>
                      <div id="FilesCompleted">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#FilesCompleted", jsonString["master.FilesCompleted"]);
                        </script>
                      </div>
                    </th>
                  </tr>
                  <tr>                    
                    <th>Files Created</th>
                    <th>Files Freed</th>
                  </tr>
                  <tr>
                    <th>
                      <div id="FilesCreated">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#FilesCreated", jsonString["master.FilesCreated"]);
                        </script>
                      </div>
                    </th>
                    <th>
                      <div id="FilesFreed">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#FilesFreed", jsonString["master.FilesFreed"]);
                        </script>
                      </div>
                    </th>
                  </tr>
                  <tr>                    
                    <th>Files Persisted</th>
                    <th>Files Pinned</th>
                  </tr>
                  <tr>
                    <th>
                      <div id="FilesPersisted">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#FilesPersisted", jsonString["master.FilesPersisted"]);
                        </script>
                      </div>
                    </th>
                    <th>
                      <div id="FilesPinned">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#FilesPinned", jsonString["master.FilesPinned"]);
                        </script>
                      </div>
                    </th>
                  </tr>
                  <tr>                    
                    <th>New Blocks Got</th>
                    <th>Paths Deleted</th>
                  </tr>
                  <tr>
                    <th>
                      <div id="NewBlocksGot">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#NewBlocksGot", jsonString["master.NewBlocksGot"]);
                        </script>
                      </div>
                    </th>
                    <th>
                      <div id="PathsDeleted">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#PathsDeleted", jsonString["master.PathsDeleted"]);
                        </script>
                      </div>
                    </th>
                  </tr>
                  
                  <tr>                    
                    <th>Paths Mounted</th>
                    <th>Paths Renamed</th>
                  </tr>
                  <tr>
                    <th>
                      <div id="PathsMounted">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#PathsMounted", jsonString["master.PathsMounted"]);
                        </script>
                      </div>
                    </th>
                    <th>
                      <div id="PathsRenamed">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#PathsRenamed", jsonString["master.PathsRenamed"]);
                        </script>
                      </div>
                    </th>
                  </tr>
                  <tr>                    
                    <th>PathsUnmounted</th>
                    <th></th>
                  </tr>
                  <tr>
                    <th>
                      <div id="PathsUnmounted">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#PathsUnmounted", jsonString["master.PathsUnmounted"]);
                        </script>
                      </div>
                    </th>
                    <th>
                    </th>
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