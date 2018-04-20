@REM
@REM The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
@REM (the "License"). You may not use this work except in compliance with the License, which is
@REM available at www.apache.org/licenses/LICENSE-2.0
@REM
@REM This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
@REM either express or implied, as more fully set forth in the License.
@REM
@REM See the NOTICE file distributed with this work for information regarding copyright ownership.
@REM

@REM ----------------------------------------------------------------------------
@REM Maven2 Start Up Batch script to run mvn.cmd with the following additional
@REM Java VM settings:
@REM
@REM     -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000
@REM
@REM ----------------------------------------------------------------------------

@setlocal
@set MAVEN_DEBUG_OPTS=-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000

@call "%~dp0"mvn.cmd %*
