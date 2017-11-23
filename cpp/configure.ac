#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([liballuxio], [0.1.120], [huang@cs.jhu.edu])
AC_CONFIG_MACRO_DIR([m4])

AC_CANONICAL_HOST
AC_CANONICAL_BUILD
AC_CANONICAL_TARGET

AC_LANG([C++])

AM_INIT_AUTOMAKE(foreign)
AC_CONFIG_FILES([Makefile
                 src/Makefile])

LT_PREREQ([2.2])
LT_INIT
LT_LANG([C++])
AX_CXX_COMPILE_STDCXX([11],,[mandatory])
LT_OUTPUT

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC

AC_ARG_VAR([JAVA_HOME], [Java Development Kit (JDK) Home Directory])

AC_MSG_CHECKING([JAVA_HOME])
if test "x$JAVA_HOME" != x; then
  AC_MSG_RESULT([$JAVA_HOME])
else
  AC_PATH_PROG([JAVAC], [javac], [$JAVAC])
  AC_PATH_PROG([JAVA], [java], [$JAVA])
  if test "x$JAVA" = "x" || test "x$JAVAC" = "x"; then
    AC_MSG_ERROR([no \$JAVA or \$JAVAC set])
  fi

  dnl Infer JAVA_HOME from Java system property
  AC_MSG_CHECKING(['java.home' system property in Java ])
    cat <<__EOF__ >conftest.java [
public class conftest {
  public static void main(String[] args) {
    System.out.print(System.getProperty("java.home"));
  }
}]
__EOF__
  
  $JAVAC conftest.java && JAVA_DOT_HOME="`$JAVA -cp . conftest`"
  if test $? = 0 && test ! -z "$JAVA_DOT_HOME"; then
    AC_MSG_RESULT($JAVA_DOT_HOME)
  else
    JAVA_DOT_HOME=""
    AC_MSG_RESULT([not found])
  fi
  
  # Clean-up
  rm -f conftest.java conftest.class

  if test -f $JAVA_DOT_HOME/bin/javac; then
    JAVA_HOME=$JAVA_DOT_HOME
  elif test -f `dirname $JAVA_DOT_HOME`/bin/javac; then
    JAVA_HOME=`dirname $JAVA_DOT_HOME`
  fi

  if test "x$JAVA_HOME" = x; then
    AC_MSG_ERROR([could not guess JAVA_HOME])
  else
    AC_MSG_NOTICE([using JAVA_HOME=$JAVA_HOME])
  fi
fi

AC_ARG_VAR([JNI_INCLUDES], [JNI header file include CXX flags])
AC_ARG_VAR([JNI_LDFLAGS], [JNI library linker flags])

case "${target_os}" in
  linux*)
    OS_NAME=linux
    ;;
  darwin*)
    OS_NAME=darwin
    ;;
  *)
    AC_MSG_ERROR("OS is not supported.")
    ;;
esac

AC_MSG_CHECKING([JNI_INCLUDES])
if test "x$JNI_INCLUDES" != x; then
  AC_MSG_RESULT([$JNI_INCLUDES])
else
  # Set JNI_INCLUDES if not set yet
  JNI_INCLUDES="-I$JAVA_HOME/include -I$JAVA_HOME/include/$OS_NAME"
fi

AC_MSG_CHECKING([JNI_LDFLAGS])
if test "x$JNI_LDFLAGS" != x; then
  AC_MSG_RESULT([$JNI_LDFLAGS])
else
  # Set JNI_LDFLAGS if not set yet
  if test "$OS_NAME" = "darwin"; then
    # JNI_LDFLAGS="-framework JavaVM"
    # using this framework importing style on OSX will likely
    # wind up loading a low version jvm even when it's configured 
    # with higher version one. Nasty.
    # fall back to the old ldflag style, (require setting LD_LIBRARY_PATH)
    dir="$JAVA_HOME/jre/lib/server"
    JNI_LDFLAGS="-L$dir -ljvm"
  elif test "$OS_NAME" = "linux"; then
    JNI_LDFLAGS=""
    for arch in i386 amd64; do
      dir="$JAVA_HOME/jre/lib/$arch/server"
      if test -e "$dir"; then
        JNI_LDFLAGS="-L$dir -R$dir -Wl,-ljvm"
        break;
      fi
    done
  fi
fi

if test "x$JNI_LDFLAGS" = x; then
  AC_MSG_ERROR([could not determine \$JNI_LDFLAGS])
fi

includedire="-I/home/innkp/cpp/include"


# Check whether we can build JNI
AC_MSG_CHECKING([if we can build with JNI])
cat <<__EOF__ >conftest.cc [
#include <jni.h>
int main()
{
  JNIEnv* env;
  JavaVM* jvm;
  JavaVMInitArgs vmArgs;
  return JNI_CreateJavaVM(&jvm, (void**) &env, &vmArgs);
}]
__EOF__

# Try to compile and link via libtool (the one we generate).
./libtool --tag=CXX --mode=link \
  $CXX $JNI_INCLUDES -o conftest conftest.cc $JNI_LDFLAGS >&5

if test $? != 0; then
  AC_MSG_ERROR([fail to build with JNI])
else
  AC_MSG_RESULT([yes])
fi

rm -f conftest

# Checks for header files.
AC_CHECK_HEADERS([stdint.h stdlib.h string.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_CHECK_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_SIZE_T
AC_TYPE_UINT32_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([mkdir strdup])

AC_SUBST(PRODUCT_MK)
AC_OUTPUT
