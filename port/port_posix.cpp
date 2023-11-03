// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "port_posix.h"
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <pthread.h>

namespace calicodb::port
{

namespace
{
// Call a pthread_*() function that will never fail given proper use.
void pthread_call(const char *name, int rc)
{
    if (rc) {
        std::fprintf(stderr, "%s: %s", name, strerror(errno));
        std::abort();
    }
}
} // namespace

#define PTHREAD_CALL(f, ...) pthread_call(#f, f(__VA_ARGS__))

Mutex::~Mutex()
{
    PTHREAD_CALL(pthread_mutex_destroy, &m_mu);
}

void Mutex::lock()
{
    PTHREAD_CALL(pthread_mutex_lock, &m_mu);
}

void Mutex::unlock()
{
    PTHREAD_CALL(pthread_mutex_unlock, &m_mu);
}

} // namespace calicodb::port
