<%--
~ The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
~ (the “License”). You may not use this work except in compliance with the License, which is
~ available at www.apache.org/licenses/LICENSE-2.0
~
~ This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
~ either express or implied, as more fully set forth in the License.
~
~ See the NOTICE file distributed with this work for information regarding copyright ownership.
--%>

<script type="text/javascript">
  var nTotalFile = <%= request.getAttribute("inMemoryFileNum") %>;

  // default view settings
  var nFilePerPage = 20;
  var nMaxPageShown = 10;

  var baseUrl = "/memory";
  var nFilePerPageCookie = "nFilePerPageMemory";
  var nMaxPageShownCookie = "nMaxPageShownMemory";

  function constructLink(off, lim) {
    return baseUrl + "?offset=" + off + "&limit=" + lim;
  }

