
# tool macros
CC := gcc
CCFLAGS := -Wall -shared -fPIC

INC = -I /usr/include -I $(JAVA_HOME)/include -I $(JAVA_HOME)/include/linux  -I $(JAVA_HOME)/include/darwin

LIBS = `pkg-config fuse --cflags --libs`

CXXFLAGS = ${CCFLAGS} ${INC} ${LIBS}

TARGET := libjnifuse.so

OBJS = JniFuseHelper.o

${TARGET}: ${OBJS}
	${CC} ${CXXFLAGS} ${INC} -o $@ -c $<

install:
	${CC} ${CXXFLAGS} -o ${TARGET} JniFuseHelper.c
	sudo mv ${TARGET} /usr/lib/

clean:
	rm -rf ${OBJS}

%.o: %.c
	${CC} ${CXXFLAGS} ${INC} -o $@ -c $<
