#include <stdio.h>

// #define DEBUG 

#ifdef DEBUG
#define LOGD(format, ...) \
do { \
    printf(format, ##__VA_ARGS__); \
} while (0)
#else
#define LOGD(format, ...)
#endif

#define LOGE(format, ...) \
do { \
    fprintf(stderr, "ERROR: " format, ##__VA_ARGS__); \
} while (0)
