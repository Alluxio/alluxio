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

FUSE2_LIBS := $(shell pkg-config fuse --libs)
FUSE2_CXXFLAGS := $(shell pkg-config fuse --cflags) -DFUSE_USE_VERSION=29

FUSE3_LIBS := $(shell pkg-config fuse3 --libs)
FUSE3_CXXFLAGS := $(shell pkg-config fuse3 --cflags) -DFUSE_USE_VERSION=30

# Linux
ifeq ($(UNAME), Linux)
	INCDIR += -I $(JAVA_HOME)/include/linux
	FUSE2_TARGET_LIB := libjnifuse.so
	FUSE3_TARGET_LIB := libjnifuse3.so
endif
# MacOS
ifeq ($(UNAME), Darwin)
	INCDIR += -I $(JAVA_HOME)/include/darwin
	FUSE2_TARGET_LIB := libjnifuse.dylib
	FUSE3_TARGET_LIB := libjnifuse3.dylib
	INCDIR += -D__APPLE__
endif

COMMON_FLAGS = -std=c++11 -Wall -fPIC -D_FILE_OFFSET_BITS=64 ${INCDIR}

FUSE2_CXXFLAGS += ${COMMON_FLAGS}
FUSE2_LDFLAGS = -shared ${FUSE2_LIBS}

FUSE3_CXXFLAGS += ${COMMON_FLAGS}
FUSE3_LDFLAGS = -shared ${FUSE3_LIBS}

OUTPUT_DIR ?= .
SRC_DIR ?= ../../src/main/native/libjnifuse
FUSE2_OBJS= $(addprefix $(OUTPUT_DIR)/, $(patsubst %.cc,%.fuse2.o,$(notdir $(wildcard $(SRC_DIR)/*.cc))))
FUSE3_OBJS= $(addprefix $(OUTPUT_DIR)/, $(patsubst %.cc,%.fuse3.o,$(notdir $(wildcard $(SRC_DIR)/*.cc))))

all: ${FUSE2_TARGET_LIB} ${FUSE3_TARGET_LIB}

${OUTPUT_DIR}/%.fuse2.o: ${SRC_DIR}/%.cc
	${CXX} ${FUSE2_CXXFLAGS} -o $@ -c $<

${FUSE2_TARGET_LIB}: ${FUSE2_OBJS}
	${CXX} ${FUSE2_CXXFLAGS} -o ${OUTPUT_DIR}/${FUSE2_TARGET_LIB} ${FUSE2_OBJS} ${FUSE2_LDFLAGS}

${OUTPUT_DIR}/%.fuse3.o: ${SRC_DIR}/%.cc
	${CXX} ${FUSE3_CXXFLAGS} -o $@ -c $<

${FUSE3_TARGET_LIB}: ${FUSE3_OBJS}
	${CXX} ${FUSE3_CXXFLAGS} -o ${OUTPUT_DIR}/${FUSE3_TARGET_LIB} ${FUSE3_OBJS} ${FUSE3_LDFLAGS}

install: ${FUSE2_TARGET_LIB} ${FUSE3_TARGET_LIB}
	sudo mv ${FUSE2_TARGET_LIB} /usr/lib/
	sudo mv ${FUSE3_TARGET_LIB} /usr/lib/

clean:
	rm -rf ${FUSE2_OBJS} ${FUSE3_OBJS} ${OUTPUT_DIR}/${FUSE2_TARGET_LIB} ${OUTPUT_DIR}/${FUSE3_TARGET_LIB}
