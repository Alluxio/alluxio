@echo off
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem     http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.

rem ---------------------------------------------------------------------------
rem Set CLASSPATH and Java options
rem ---------------------------------------------------------------------------

rem Make sure prerequisite environment variables are set
if not "%JAVA_HOME%" == "" goto gotJdkHome
if not "%JRE_HOME%" == "" goto gotJreHome
echo Neither the JAVA_HOME nor the JRE_HOME environment variable is defined
echo At least one of these environment variable is needed to run this program
goto exit

:gotJreHome
if not exist "%JRE_HOME%\bin\java.exe" goto noJavaHome
if not exist "%JRE_HOME%\bin\javaw.exe" goto noJavaHome
if not ""%1"" == ""debug"" goto okJavaHome
echo JAVA_HOME should point to a JDK in order to run in debug mode.
goto exit

:gotJdkHome
if not exist "%JAVA_HOME%\bin\java.exe" goto noJavaHome
if not exist "%JAVA_HOME%\bin\javaw.exe" goto noJavaHome
if not exist "%JAVA_HOME%\bin\jdb.exe" goto noJavaHome
if not exist "%JAVA_HOME%\bin\javac.exe" goto noJavaHome
if not "%JRE_HOME%" == "" goto okJavaHome
set "JRE_HOME=%JAVA_HOME%"
goto okJavaHome

:noJavaHome
echo The JAVA_HOME environment variable is not defined correctly
echo This environment variable is needed to run this program
echo NB: JAVA_HOME should point to a JDK not a JRE
goto exit
:okJavaHome

if not "%BASEDIR%" == "" goto gotBasedir
echo The BASEDIR environment variable is not defined
echo This environment variable is needed to run this program
goto exit
:gotBasedir
if exist "%BASEDIR%\bin\setclasspath.bat" goto okBasedir
echo The BASEDIR environment variable is not defined correctly
echo This environment variable is needed to run this program
goto exit
:okBasedir

rem Don't override the endorsed dir if the user has set it previously
if not "%JAVA_ENDORSED_DIRS%" == "" goto gotEndorseddir
rem Set the default -Djava.endorsed.dirs argument
set "JAVA_ENDORSED_DIRS=%BASEDIR%\endorsed"
:gotEndorseddir

rem Don't override _RUNJAVA if the user has set it previously
if not "%_RUNJAVA%" == "" goto gotRunJava
rem Set standard command for invoking Java.
rem Also note the quoting as JRE_HOME may contain spaces.
set _RUNJAVA="%JRE_HOME%\bin\java.exe"
:gotRunJava

rem Don't override _RUNJDB if the user has set it previously
rem Also note the quoting as JAVA_HOME may contain spaces.
if not "%_RUNJDB%" == "" goto gotRunJdb
set _RUNJDB="%JAVA_HOME%\bin\jdb.exe"
:gotRunJdb

goto end

:exit
exit /b 1

:end
exit /b 0
