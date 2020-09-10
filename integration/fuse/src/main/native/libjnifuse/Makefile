#
# The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
# (the "License"). You may not use this work except in compliance with the License, which is
# available at www.apache.org/licenses/LICENSE-2.0
#
# This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied, as more fully set forth in the License.
#
# See the NOTICE file distributed with this work for information regarding copyright ownership.

CXX := g++
UNAME := $(shell uname)
INCDIR = -I /usr/include -I $(JAVA_HOME)/include -I ./
FUSE_LIBS = $(shell pkg-config fuse --libs)
FUSE_CXXFLAGS = $(shell pkg-config fuse --cflags)

# Linux
ifeq ($(UNAME), Linux)
	INCDIR += -I $(JAVA_HOME)/include/linux
	TARGET_LIB := libjnifuse.so
endif
# MacOS
ifeq ($(UNAME), Darwin)
	INCDIR += -I $(JAVA_HOME)/include/darwin
	TARGET_LIB := libjnifuse.dylib
endif

CXXFLAGS = -std=c++11 -Wall -fPIC -D_FILE_OFFSET_BITS=64 ${INCDIR} ${FUSE_CXXFLAGS}
LDFLAGS = -shared ${FUSE_LIBS}
OBJS = jnifuse_helper.o jnifuse_fs.o jnifuse_impls.o operation.o

${TARGET_LIB}: ${OBJS}
	${CXX} ${CXXFLAGS} -o ${TARGET_LIB} ${OBJS} ${LDFLAGS}

install: ${TARGET_LIB}
	sudo mv ${TARGET_LIB} /usr/lib/

clean:
	rm -rf ${OBJS} ${TARGET_LIB}

%.o: %.cc
	${CXX} ${CXXFLAGS} -o $@ -c $<
