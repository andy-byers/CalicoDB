// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_JSON_H
#define CALICODB_JSON_H

#include "calicodb/slice.h"
#include "calicodb/status.h"

namespace calicodb::json
{

class Handler
{
public:
    explicit Handler();
    virtual ~Handler();

    [[nodiscard]] virtual auto accept_key(const Slice &value) -> bool = 0;
    [[nodiscard]] virtual auto accept_string(const Slice &value) -> bool = 0;
    [[nodiscard]] virtual auto accept_integer(int64_t value) -> bool = 0;
    [[nodiscard]] virtual auto accept_real(double value) -> bool = 0;
    [[nodiscard]] virtual auto accept_boolean(bool value) -> bool = 0;
    [[nodiscard]] virtual auto accept_null() -> bool = 0;
    [[nodiscard]] virtual auto begin_object() -> bool = 0;
    [[nodiscard]] virtual auto end_object() -> bool = 0;
    [[nodiscard]] virtual auto begin_array() -> bool = 0;
    [[nodiscard]] virtual auto end_array() -> bool = 0;
};

class Reader
{
public:
    explicit Reader(Handler &h)
        : m_handler(&h)
    {
    }

    auto read(const Slice &input) -> Status;

private:
    Handler *const m_handler;
};

} // namespace calicodb::json

#endif // CALICODB_JSON_H
