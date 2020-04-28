CC := g++
UNAME := $(shell uname)
INCDIR = -I /usr/include -I $(JAVA_HOME)/include
FUSE_LIBS = $(shell pkg-config fuse --libs)
FUSE_CFLAGS = $(shell pkg-config fuse --cflags)

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

CFLAGS = -Wall -fPIC ${INCDIR} ${FUSE_CFLAGS}
LDFLAGS = -shared ${FUSE_LIBS}
OBJS = JniFuseHelper.o

${TARGET_LIB}: ${OBJS}
	${CC} ${CFLAGS} ${LDFLAGS} -o $@ $<

install: ${TARGET_LIB}
	sudo mv ${TARGET_LIB} /usr/lib/

clean:
	rm -rf ${OBJS} ${TARGET_LIB}

%.o: %.cc
	${CC} ${CFLAGS} -o $@ -c $<
