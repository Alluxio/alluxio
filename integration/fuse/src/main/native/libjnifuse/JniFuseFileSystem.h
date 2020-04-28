#ifndef _JNI_FUSE_FILE_SYSTEM_H
#define _JNI_FUSE_FILE_SYSTEM_H

#include "jni.h"

namespace jnifuse {

class Operation;
class GetattrOperation;
class OpenOperation;
class ReadOperation;
class ReaddirOperation;
    
class JniFuseFileSystem
{
private:
    JniFuseFileSystem();
    ~JniFuseFileSystem();
public:
    static JniFuseFileSystem* getInstance();
    void init(JNIEnv* env, jobject obj);
    JNIEnv* getEnv();
    jobject getFSObj();
private:
    JavaVM* jvm;
    jobject fs;
public:
    GetattrOperation *getattrOper;
    OpenOperation* openOper;
    ReadOperation* readOper;
    ReaddirOperation* readdirOper;
};

class Operation
{
protected:
    JniFuseFileSystem* fs;
    jclass clazz;
    jobject obj;
    jmethodID methodID;
    const char *signature;
public:
    Operation();
    ~Operation();
};

class GetattrOperation: public Operation {
public:
    GetattrOperation(JniFuseFileSystem* fs);
    int call(const char *path, struct stat* stbuf);
};

class OpenOperation: public Operation {
public:
    OpenOperation(JniFuseFileSystem* fs);
    int call(const char *path, struct fuse_file_info *fi);
};

class ReadOperation: public Operation {
public:
    ReadOperation(JniFuseFileSystem* fs);
    int call(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
};

class ReaddirOperation: public Operation {
public:
    ReaddirOperation(JniFuseFileSystem* fs);
    int call(const char* path, void* buf, fuse_fill_dir_t filler,
            off_t offset, struct fuse_file_info* fi);
private:
    jclass fillerclazz;
    jmethodID fillerconstructor;
};

}

#endif