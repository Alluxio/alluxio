#include <stdio.h>

#define DEBUG 

#ifdef DEBUG
#define LOGD(format, ...) \
do { \
    fprintf(stdout, "DEBUG %s:%d " format "\n", \
        __FILE__, __LINE__, ##__VA_ARGS__); \
} while (0)
#else
#define LOGD(format, ...)
#endif

#define LOGE(format, ...) \
do { \
    fprintf(stderr, "ERROR %s:%d " format "\n", \
        __FILE__, __LINE__, ##__VA_ARGS__); \
} while (0)
