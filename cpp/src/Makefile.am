# Adds src files needed to be compiled
lib_LTLIBRARIES = liballuxio.la
liballuxio_la_SOURCES = FileSystem.cc JNIHelper.cc Status.cc FileInStream.cc FileOutStream.cc
liballuxio_la_CPPFLAGS = $(JNI_INCLUDES)
liballuxio_la_LIBADD = $(JNI_LDFLAGS)

# Adds include dir path
INCLUDES = -I$(top_srcdir)/include
include_liballuxiodir = $(includedire)

# Builds test bin file
bin_PROGRAMS = alluxiotest

alluxiotest_SOURCES = FileSystemTest.cc 
alluxiotest_CPPFLAGS = $(JNI_INCLUDES)
alluxiotest_LDADD = liballuxio.la -lpthread
