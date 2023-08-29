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
INCDIR = -I /usr/include -I $(JAVA_HOME)/include

FUSE2_LIBS := $(shell pkg-config fuse --libs)
FUSE2_CXXFLAGS := $(shell pkg-config fuse --cflags) -DFUSE_USE_VERSION=29

# Linux
ifeq ($(UNAME), Linux)
	INCDIR += -I $(JAVA_HOME)/include/linux
	FUSE2_CLI := fuse2cli
endif
# MacOS
ifeq ($(UNAME), Darwin)
	INCDIR += -I $(JAVA_HOME)/include/darwin
	FUSE2_CLI := fuse2cli
	INCDIR += -D__APPLE__
endif

COMMON_FLAGS = -std=c++11 -Wall -fPIC -D_FILE_OFFSET_BITS=64 ${INCDIR}

OUTPUT_DIR ?= .
CLI_SRC_DIR ?= ../../src/main/native/clijnifuse
CLI_FUSE2_COMMAND_OBJS = $(addprefix $(OUTPUT_DIR)/, $(patsubst %.cc,%.cli.o,$(notdir $(wildcard $(CLI_SRC_DIR)/command/*.cc))))
CLI_FUSE2_OBJS = $(addprefix $(OUTPUT_DIR)/, $(patsubst %.cc,%.cli.o,$(notdir $(wildcard $(CLI_SRC_DIR)/*.cc)))) ${CLI_FUSE2_COMMAND_OBJS}

all: ${FUSE2_CLI}

${OUTPUT_DIR}/%.cli.o: ${CLI_SRC_DIR}/%.cc
	${CXX} ${COMMON_FLAGS} ${FUSE2_CXXFLAGS} -o $@ -c $<

${OUTPUT_DIR}/%.cli.o: ${CLI_SRC_DIR}/command/%.cc
	${CXX} ${COMMON_FLAGS} ${FUSE2_CXXFLAGS} -I ${CLI_SRC_DIR}/command/include -o $@ -c $<

${FUSE2_CLI}: ${CLI_FUSE2_OBJS}
	${CXX} ${COMMON_FLAGS} ${FUSE2_CXXFLAGS} -o ${OUTPUT_DIR}/${FUSE2_CLI} ${CLI_FUSE2_OBJS} ${FUSE2_LIBS}


clean:
	rm -rf ${CLI_FUSE2_OBJS} ${CLI_FUSE2_COMMAND_OBJS} ${FUSE2_CLI}
