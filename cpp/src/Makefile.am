# Adds src files needed to be compiled
lib_LTLIBRARIES = liballuxio.la
liballuxio_la_SOURCES = FileSystem.cpp JNIHelper.cpp Status.cpp FileInStream.cpp FileOutStream.cpp
liballuxio_la_CPPFLAGS = $(JNI_INCLUDES)
liballuxio_la_LIBADD = $(JNI_LDFLAGS)

# Adds include dir path
INCLUDES = -I$(top_srcdir)/include
include_liballuxiodir = $(includedire)

# Builds test bin file
bin_PROGRAMS = alluxiotest

alluxiotest_SOURCES = FileSystemTest.cpp 
alluxiotest_CPPFLAGS = $(JNI_INCLUDES)
alluxiotest_LDADD = liballuxio.la -lpthread
