#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.
#

cmake_minimum_required(VERSION 3.0.1 FATAL_ERROR)

SET(SOURCE_FILES
    FileSystem.cc
    JNIHelper.cc
    Status.cc
    FileInStream.cc
    FileOutStream.cc
)

INCLUDE_DIRECTORIES(
${JNI_INCLUDE_DIRS}
${CMAKE_CURRENT_SOURCE_DIR}/../include
)

ADD_LIBRARY(filesystem STATIC ${SOURCE_FILES})
ADD_LIBRARY(sharedfilesystem SHARED ${SOURCE_FILES})

TARGET_LINK_LIBRARIES(filesystem ${JNI_LIBRARIES} -lpthread)
