<%@ page import="java.util.*" %>
<%@ page import="tachyon.web.*" %>
<%@ page import="tachyon.web.*" %>

<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link href="../css/bootstrap.min.css" rel="stylesheet" media="screen">
  <link href="../css/tachyoncustom.min.css" rel="stylesheet">
  <link href="../css/linearGraph.css" rel="stylesheet">
</head>
<title>Tachyon</title>
<body>
<script src="../js/jquery-1.9.1.min.js" type="text/javascript"></script>
<script src="../js/bootstrap.min.js"></script>
<script src="../js/d3.min.js" type="text/javascript"></script>
<script src="../js/linearGraph.js" type="text/javascript"></script>
<div class="container-fluid">
  <% request.setAttribute("useWorkerHeader", "1"); %>
  <jsp:include page="/header" />

    <div class="row-fluid">
      <div class="accordion span14" id="accordion3">
        <div class="accordion-group">
          <div class="accordion-heading">
            <a class="accordion-toggle" data-toggle="collapse" data-parent="#accordion3" href="#data3">
              <h4>Worker gauges</h4>
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
                    <th>Worker Capacity</th>                    
                    <th>
                      <div class="progress custom-progress">
                        <div class="bar bar-success" style="width: <%= (Integer) request.getAttribute("workerCapacityFreePercentage") %>%;">
                          <% if ( (Integer) request.getAttribute("workerCapacityFreePercentage") >= (Integer) request.getAttribute("workerCapacityUsedPercentage")) { %>
                            <%= request.getAttribute("workerCapacityFreePercentage") %>%Free
                          <% } %>
                        </div>
                        <div class="bar bar-danger" style="width: <%= (Integer) request.getAttribute("workerCapacityUsedPercentage") %>%;">
                          <% if ((Integer) request.getAttribute("workerCapacityFreePercentage") < (Integer) request.getAttribute("workerCapacityUsedPercentage")) { %>
                            <%= request.getAttribute("workerCapacityUsedPercentage") %>%Used
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
              
              <table class="table table-condensed">
                <thead>                  
                  <th></th>                  
                  <th></th>
                </thead>
                <tbody>
                  <tr>                    
                    <th>Blocks Accessed</th>
                    <th>Blocks Cached</th>
                  </tr>
                  <tr>
                    <th>
                      <div id="BlocksAccessed">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#BlocksAccessed", jsonString["worker.BlocksAccessed"]);
                        </script>
                      </div>
                      
                    </th>
                    <th>
                      <div id="BlocksCached">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#BlocksCached", jsonString["worker.BlocksCached"]);
                        </script>
                      </div>
                    </th>
                  </tr>
                  <tr>                    
                    <th>Blocks Canceled</th>
                    <th>Blocks Deleted</th>
                  </tr>
                  <tr>
                    <th>
                      <div id="BlocksCanceled">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#BlocksCanceled", jsonString["worker.BlocksCanceled"]);
                        </script>
                      </div>
                      
                    </th>
                    <th>
                      <div id="BlocksDeleted">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#BlocksDeleted", jsonString["worker.BlocksDeleted"]);
                        </script>
                      </div>
                    </th>
                  </tr>
                  <tr>                    
                    <th>Blocks Evicted</th>
                    <th>Blocks Promoted</th>
                  </tr>
                  <tr>
                    <th>
                      <div id="BlocksEvicted">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#BlocksEvicted", jsonString["worker.BlocksEvicted"]);
                        </script>
                      </div>
                    </th>
                    <th>
                      <div id="BlocksPromoted">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#BlocksPromoted", jsonString["worker.BlocksPromoted"]);
                        </script>
                      </div>
                    </th>
                  </tr>
                  <tr>                    
                    <th>Blocks Read Locally</th>
                    <th>Blocks Read Remotely</th>
                  </tr>
                  <tr>
                    <th>
                      <div id="BlocksReadLocal">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#BlocksReadLocal", jsonString["worker.BlocksReadLocal"]);
                        </script>
                      </div>
                    </th>
                    <th>
                      <div id="BlocksReadRemote">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#BlocksReadRemote", jsonString["worker.BlocksReadRemote"]);
                        </script>
                      </div>
                    </th>
                  </tr>
                  <tr>                    
                    <th>Blocks Written Locally</th>
                    <th>Bytes Read Locally</th>
                  </tr>
                  <tr>
                    <th>
                      <div id="BlocksWrittenLocal">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#BlocksWrittenLocal", jsonString["worker.BlocksWrittenLocal"]);
                        </script>
                      </div>
                    </th>
                    <th>
                      <div id="BytesReadLocal">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#BytesReadLocal", jsonString["worker.BytesReadLocal"]);
                        </script>
                      </div>
                    </th>
                  </tr>
                  
                  <tr>                    
                    <th>Bytes Read Remotely</th>
                    <th>Underfs Bytes read</th>
                  </tr>
                  <tr>
                    <th>
                      <div id="BytesReadRemote">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#BytesReadRemote", jsonString["worker.BytesReadRemote"]);
                        </script>
                      </div>
                    </th>
                    <th>
                      <div id="BytesReadUfs">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#BytesReadUfs", jsonString["worker.BytesReadUfs"]);
                        </script>
                      </div>
                    </th>
                  </tr>
                  <tr>                    
                    <th>Bytes Written Locally</th>
                    <th>Underfs Bytes Written</th>
                  </tr>
                  <tr>
                    <th>
                      <div id="BytesWrittenLocal">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#BytesWrittenLocal", jsonString["worker.BytesWrittenLocal"]);
                        </script>
                      </div>
                    </th>
                    <th>
                      <div id="BytesWrittenUfs">                
                        <script type="text/javascript">
                        var jsonString = ${operationMetrics};
                        linearGraph("#BytesWrittenUfs", jsonString["worker.BytesWrittenUfs"]);
                        </script>
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
  <%@ include file="../footer.jsp" %>
</div>
</body>
</html>