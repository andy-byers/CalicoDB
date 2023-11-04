// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_DSL_H
#define CALICODB_DSL_H

#include "buffer.h"
#include "calicodb/slice.h"
#include "calicodb/status.h"

namespace calicodb
{

enum Event {
    kEventValueString,
    kEventValueInteger,
    kEventValueReal,
    kEventValueBoolean,
    kEventValueNull,
    kEventBeginObject,
    kEventEndObject,
    kEventBeginArray,
    kEventEndArray,
    kEventKey, // Special event for object key
    kEventCount
};

union Value {
    void *null = nullptr;
    bool boolean;
    int64_t integer;
    double real;
    Slice string;
};

using Action = void (*)(void *, const Value *);

class DSLReader
{
public:
    explicit DSLReader() = default;
    auto register_action(Event event, const Action &action) -> void;
    auto read(const Slice &input, void *action_arg) -> Status;

private:
    auto dispatch(Event event, void *action_arg, const Value *value) -> void;

    Action m_actions[kEventCount] = {};
};

template <class Target>
class OutputStream
{
};

template <>
class OutputStream<Buffer<char>>
{
public:
    explicit OutputStream(Buffer<char> &buffer)
        : m_buffer(&buffer)
    {
    }

    [[nodiscard]] auto size() const -> size_t
    {
        return m_size;
    }

    void reset()
    {
        m_size = 0;
    }

    auto write(const Slice &data) -> Status
    {
        if (!data.is_empty()) {
            const auto needed_size = m_size + data.size();
            if (needed_size > m_buffer->size()) {
                auto capacity = m_buffer->size() + m_buffer->is_empty();
                while (capacity < needed_size) {
                    capacity *= 2;
                }
                if (m_buffer->resize(capacity)) {
                    return Status::no_memory();
                }
            }
            std::memcpy(m_buffer->data() + m_size, data.data(), data.size());
            m_size += data.size();
        }
        return Status::ok();
    }

private:
    Buffer<char> *const m_buffer;
    uint64_t m_size = 0;
};

template <>
class OutputStream<File>
{
public:
    explicit OutputStream(File &file)
        : m_file(&file)
    {
    }

    [[nodiscard]] auto size() const -> size_t
    {
        return m_size;
    }

    auto write(const Slice &data) -> Status
    {
        Status s;
        if (m_size == 0) {
            s = m_file->get_size(m_size);
        }
        if (s.is_ok()) {
            s = m_file->write(m_size, data);
        }
        if (s.is_ok()) {
            m_size += data.size();
        }
        return s;
    }

private:
    File *const m_file;
    uint64_t m_size = 0;
};

template <class OS>
class DSLWriter
{
public:
    explicit DSLWriter(OS &os)
        : m_os(&os)
    {
    }

    [[nodiscard]] auto is_ok() const -> bool
    {
        return m_status.is_ok();
    }

    [[nodiscard]] auto status() const -> const Status &
    {
        return m_status;
    }

    struct Position {
        uint32_t index;
        bool is_object;
    };

    [[nodiscard]] auto position() const -> Position
    {
        return m_position;
    }

    [[nodiscard]] auto depth() const -> Position
    {
        return m_stack.size();
    }

    auto reset() -> void
    {
        m_status = Status::ok();
        m_key_flag = true;
        m_stack.clear();
        m_position = {};
    }

    auto write_key(const Slice &value) -> DSLWriter &
    {
        CALICODB_EXPECT_FALSE(m_stack.is_empty());
        CALICODB_EXPECT_TRUE(m_stack.back().is_object);
        start_next_write();
        write_to_stream("\"");
        write_to_stream(value);
        write_to_stream("\": ");
        m_key_flag = is_ok();
        return *this;
    }

    auto write_string(const Slice &value) -> DSLWriter &
    {
        start_next_write();
        write_to_stream("\"");
        write_to_stream(value);
        write_to_stream("\"");
        advance_index();
        return *this;
    }

    auto write_integer(int64_t value) -> DSLWriter &
    {
        char buffer[32];
        const auto n = std::snprintf(buffer, sizeof(buffer), "%ld", value);
        CALICODB_EXPECT_GE(n, 0); // snprintf() shouldn't fail

        start_next_write();
        write_to_stream(Slice(buffer, n));
        advance_index();
        return *this;
    }

    auto write_real(double value) -> DSLWriter &
    {
        char buffer[32];
        const auto n = std::snprintf(buffer, sizeof(buffer), "%lf", value);
        CALICODB_EXPECT_GE(n, 0); // snprintf() shouldn't fail

        start_next_write();
        write_to_stream(Slice(buffer, n));
        advance_index();
        return *this;
    }

    auto write_boolean(bool value) -> DSLWriter &
    {
        start_next_write();
        static constexpr Slice kWords[] = {"false", "true"};
        write_to_stream(kWords[value]);
        advance_index();
        return *this;
    }

    auto write_null() -> DSLWriter &
    {
        start_next_write();
        write_to_stream("null");
        advance_index();
        return *this;
    }

    auto begin_object() -> DSLWriter &
    {
        start_next_write();
        enter_structure(true);
        write_to_stream("{");
        return *this;
    }

    auto end_object() -> DSLWriter &
    {
        CALICODB_EXPECT_FALSE(m_stack.is_empty());
        if (m_position.index) {
            write_newline();
            leave_structure();
            write_indentation();
        } else {
            leave_structure();
        }
        write_to_stream("}");
        advance_index();
        return *this;
    }

    auto begin_array() -> DSLWriter &
    {
        start_next_write();
        enter_structure(false);
        write_to_stream("[");
        return *this;
    }

    auto end_array() -> DSLWriter &
    {
        CALICODB_EXPECT_FALSE(m_stack.is_empty());
        if (m_position.index) {
            write_newline();
            leave_structure();
            write_indentation();
        } else {
            leave_structure();
        }
        write_to_stream("]");
        return *this;
    }

private:
    auto write_to_stream(const Slice &data) -> void
    {
        if (m_status.is_ok()) {
            m_status = m_os->write(data);
        }
    }

    auto advance_index() -> void
    {
        m_position.index += m_status.is_ok();
    }

    auto start_next_write() -> void
    {
        if (m_key_flag) {
            m_key_flag = false;
            return;
        }
        if (m_position.index) {
            write_to_stream(",");
        }
        write_to_stream("\n");
        write_indentation();
    }

    auto enter_structure(bool is_object) -> void
    {
        CALICODB_EXPECT_TRUE(m_status.is_ok());
        m_position.is_object = is_object;
        if (m_stack.push_back(m_position)) {
            m_status = Status::no_memory();
        }
        m_position.index = 0;
    }

    auto leave_structure() -> void
    {
        CALICODB_EXPECT_FALSE(m_stack.is_empty());
        m_position = m_stack.back();
        m_stack.pop_back();
        ++m_position.index;
    }

    auto write_newline() -> void
    {
        if (m_status.is_ok()) {
            m_status = m_os->write("\n");
        }
    }

    auto write_indentation() -> void
    {
        static constexpr size_t kIndentWidth = 2;
        static constexpr char kIndent[kIndentWidth + 1] = "  ";
        for (size_t i = 0; i < m_stack.size() && m_status.is_ok(); ++i) {
            m_status = m_os->write(Slice(kIndent, kIndentWidth));
        }
    }

    OS *const m_os;
    Status m_status;
    Vector<Position> m_stack;
    Position m_position = {};
    bool m_key_flag = true;
};

} // namespace calicodb

#endif // CALICODB_DSL_H
