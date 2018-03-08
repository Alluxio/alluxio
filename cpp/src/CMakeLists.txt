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
    fileSystem.cc
    jniHelper.cc
    status.cc
    fileInStream.cc
    fileOutStream.cc
)

SET(CPP_LIBRAIY_NAME "alluxio${ALLUXIO_CPP_VERSION}")

INCLUDE_DIRECTORIES(
${JNI_INCLUDE_DIRS}
${CMAKE_CURRENT_SOURCE_DIR}/../include
)

ADD_LIBRARY(alluxio SHARED  ${SOURCE_FILES})
ADD_LIBRARY(alluxio_static STATIC  ${SOURCE_FILES})

SET_TARGET_PROPERTIES(alluxio_static PROPERTIES OUTPUT_NAME ${CPP_LIBRAIY_NAME})
SET_TARGET_PROPERTIES(alluxio PROPERTIES OUTPUT_NAME ${CPP_LIBRAIY_NAME})

#TARGET_LINK_LIBRARIES(alluxio ${JNI_LIBRARIES} -lpthread)
