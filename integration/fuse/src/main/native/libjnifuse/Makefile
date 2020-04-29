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

CXXFLAGS = -Wall -fPIC -D_FILE_OFFSET_BITS=64 ${INCDIR} ${FUSE_CXXFLAGS}
LDFLAGS = -shared ${FUSE_LIBS}
OBJS = JniFuseHelper.o JniFuseFileSystem.o

${TARGET_LIB}: ${OBJS}
	${CXX} ${CXXFLAGS} ${LDFLAGS} -o ${TARGET_LIB} ${OBJS}

install: ${TARGET_LIB}
	sudo mv ${TARGET_LIB} /usr/lib/

clean:
	rm -rf ${OBJS} ${TARGET_LIB}

%.o: %.cc
	${CXX} ${CXXFLAGS} -o $@ -c $<
