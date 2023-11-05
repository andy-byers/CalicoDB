// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_JSON_H
#define CALICODB_JSON_H

#include "calicodb/slice.h"
#include "calicodb/status.h"
#include "mem.h"

namespace calicodb::json
{

struct Node;

enum class Error {
    kNone,
    kNoMemory,
    kInvalidDocument,
    kInvalidEscape,
    kInvalidCodepoint,
    kInvalidLiteral,
    kInvalidString,
    kInvalidNumber,
    kInvalidComment,
    kExceededMaxDepth,
};

enum class Type {
    kKey,
    kString,
    kInteger,
    kReal,
    kBoolean,
    kNull,
    kObject,
    kArray,
};

struct [[nodiscard]] Result {
    size_t line;
    size_t column;
    Error error;

    explicit operator bool() const
    {
        return error == Error::kNone;
    }
};

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

    auto read(const Slice &input) -> Result;

    Reader(Reader &) = delete;
    void operator=(Reader &) = delete;

private:
    Handler *const m_handler;
};

class Document final : public HeapObject
{
public:
    ~Document();

    // TODO: Write an API. The following method should be removed eventually. It is just for testing.
    [[nodiscard]] auto TODO_render_to_std_string() const -> std::string;

    Document(const Document &) = delete;
    auto operator=(const Document &) -> Document & = delete;
    Document(Document &&rhs) noexcept;
    auto operator=(Document &&rhs) noexcept -> Document &;

private:
    friend auto new_document(const Slice &, Document *&) -> Result;
    friend auto new_document() -> Document *;
    class DocumentImpl;

    explicit Document(DocumentImpl &impl)
        : m_impl(&impl)
    {
    }

    DocumentImpl *m_impl;
};

auto new_document(const Slice &input, Document *&doc_out) -> Result;
auto new_document() -> Document *;

} // namespace calicodb::json

#endif // CALICODB_JSON_H
