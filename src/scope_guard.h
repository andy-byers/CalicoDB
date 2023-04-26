// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_SCOPE_GUARD_H
#define CALICODB_SCOPE_GUARD_H

#include "utils.h"

namespace calicodb {

template<class Callback>
class ScopeGuard final
{
public:
    ScopeGuard(Callback cb)
    {
        new(m_callback) Callback(std::move(cb));
        m_constructed = true;
    }

    ~ScopeGuard()
    {
        if (m_constructed) {
            callback()();
            destroy_callback();
        }
    }

    ScopeGuard(ScopeGuard &) = delete;
    void operator=(ScopeGuard &) = delete;

    auto cancel() && -> void
    {
        CALICODB_EXPECT_TRUE(m_constructed);
        destroy_callback();
    }

    auto invoke() && -> void
    {
        CALICODB_EXPECT_TRUE(m_constructed);
        callback()();
        destroy_callback();
    }

private:
    auto destroy_callback() -> void
    {
        callback().~Callback();
        m_constructed = false;
    }
    
    [[nodiscard]] auto callback() -> Callback &
    {
        return *reinterpret_cast<Callback *>(m_callback);
    }

    alignas(Callback) char m_callback[sizeof(Callback)];
    bool m_constructed;
};

} // namespace calicodb

#endif // CALICODB_SCOPE_GUARD_H
