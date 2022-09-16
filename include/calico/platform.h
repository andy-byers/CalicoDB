#ifndef CALICO_PLATFORM_H
#define CALICO_PLATFORM_H

#if (!defined(_WIN32) && !defined(_WIN64)) && (defined(__unix__) || defined(__unix) || defined(__APPLE__))
#  define CALICO_UNIX
#  ifdef __APPLE__
#    define CALICO_OSX
#  endif
#else
#  error "error: this platform is currently unsupported"
#endif

#endif // CALICO_PLATFORM_H
