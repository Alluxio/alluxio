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

INCLUDE_DIRECTORIES(
${JNI_INCLUDE_DIRS}
${CMAKE_CURRENT_SOURCE_DIR}/../include
)

ADD_LIBRARY(FILESYSTEM
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FileSystem.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/JNIHelper.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/Status.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FileInStream.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../src/FileOutStream.cc
)

ADD_EXECUTABLE(JNIHelper_Test JNIHelperTest.cc)
ADD_EXECUTABLE(filesystem_test FileSystemTest.cc)
ADD_EXECUTABLE(fileRW_test FileRWTest.cc)

TARGET_LINK_LIBRARIES(fileRW_test FILESYSTEM ${JNI_LIBRARIES} -lpthread)
TARGET_LINK_LIBRARIES(filesystem_test FILESYSTEM ${JNI_LIBRARIES} -lpthread)
TARGET_LINK_LIBRARIES(JNIHelper_Test FILESYSTEM ${JNI_LIBRARIES} -lpthread)

ENABLE_TESTING()

ADD_TEST(NAME test_filesystem_API COMMAND filesystem_test)
ADD_TEST(NAME test_read_write_API COMMAND fileRW_test)
ADD_TEST(NAME JNIHelper_Test COMMAND JNIHelper_Test)
