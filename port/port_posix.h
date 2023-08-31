// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PORT_PORT_POSIX_H
#define CALICODB_PORT_PORT_POSIX_H

#include <pthread.h>

namespace calicodb::port
{

class Mutex final
{
public:
    explicit constexpr Mutex()
        : m_mu(PTHREAD_MUTEX_INITIALIZER)
    {
    }

    ~Mutex();

    Mutex(const Mutex &) = delete;
    auto operator=(const Mutex &) -> Mutex & = delete;

    auto lock() -> void;
    auto unlock() -> void;

private:
    pthread_mutex_t m_mu;
};

} // namespace calicodb::port

#endif // CALICODB_PORT_PORT_POSIX_H
