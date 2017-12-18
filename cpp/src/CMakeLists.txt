cmake_minimum_required(VERSION 3.0.1 FATAL_ERROR)

SET(SOURCE_FILES
FileSystem.cc 
JNIHelper.cc
Status.cc 
FileInStream.cc
FileOutStream.cc 
)

INCLUDE_DIRECTORIES(
$ENV{JAVA_HOME}/include
$ENV{JAVA_HOME}/include/linux
${CMAKE_CURRENT_SOURCE_DIR}/../include
)

LINK_DIRECTORIES(
$ENV{LD_LIBRARY_PATH}
)

ADD_LIBRARY(filesystem STATIC ${SOURCE_FILES})
ADD_LIBRARY(sharedfilesystem SHARED ${SOURCE_FILES})
target_link_libraries(filesystem libjvm.so -lpthread)

ADD_EXECUTABLE(FileSystemTest  FileSystemTest.cc)
target_link_libraries(FileSystemTest filesystem)

SET_TARGET_PROPERTIES(FileSystemTest  PROPERTIES
RUNTIME_OUTPUT_DIRECTORY  "${CMAKE_CURRENT_SOURCE_DIR}/../bin")
