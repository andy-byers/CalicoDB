// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "dsl.h"
#include "internal.h"
#include "logging.h"
#include "status_internal.h"

namespace calicodb
{

namespace
{

constexpr uint8_t kIsSpaceTable[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

constexpr uint8_t kIsHexTable[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0, 0,
    0, 11, 12, 13, 14, 15, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 11, 12, 13, 14, 15, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

constexpr auto fast_isspace(char c) -> bool
{
    return kIsSpaceTable[static_cast<uint8_t>(c)];
}

constexpr auto fast_ishex(char c) -> bool
{
    return kIsHexTable[static_cast<uint8_t>(c)];
}

constexpr auto fast_hexval(char c) -> int
{
    CALICODB_EXPECT_TRUE(fast_ishex(c));
    return kIsHexTable[static_cast<uint8_t>(c)] - 1;
}

struct Token {
    enum Kind {
        kValueString,
        kBeginObject,
        kEndObject,
        kNameSeparator,
        kValueSeparator,
        kEndOfInput,
        kParseError,
    } kind;
    const char *begin;
    const char *end;
};

class Lexer
{
public:
    struct Position {
        int line;
        int column;
    };

    explicit Lexer(const Slice &input)
        : m_begin(input.data()),
          m_itr(m_begin),
          m_end(m_itr + input.size())
    {
    }

    [[nodiscard]] auto error_msg() const -> const char *
    {
        return m_error_msg;
    }

    [[nodiscard]] auto position() const -> const Position &
    {
        return m_pos;
    }

    [[nodiscard]] auto is_empty() const -> bool
    {
        return m_itr == m_end;
    }

    [[nodiscard]] auto remaining() const -> size_t
    {
        return static_cast<size_t>(m_end - m_itr);
    }

    [[nodiscard]] auto peek() const -> char
    {
        return is_empty() ? '\0' : *m_itr;
    }

    auto get() -> char
    {
        if (m_unget) {
            m_unget = false;
        } else if (is_empty()) {
            m_char = '\0';
        } else {
            m_char = *m_itr++;
            if (m_char == '\n') {
                m_pos.column = 0;
                ++m_pos.line;
            } else {
                ++m_pos.column;
            }
        }
        return m_char;
    }

    auto unget() -> void
    {
        assert(!m_unget);
        assert(m_itr != m_begin);
        m_unget = true;
    }

    [[nodiscard]] auto scan() -> Token
    {
        skip_whitespace();
        skip_comments();
        skip_whitespace();

        for (;;) {
            switch (get()) {
                case '{':
                    return make_token(Token::kBeginObject, 1);
                case '}':
                    return make_token(Token::kEndObject, 1);
                case ':':
                    return make_token(Token::kNameSeparator, 1);
                case ',':
                    return make_token(Token::kValueSeparator, 1);
                case '"':
                    return scan_string();
                case '\0':
                case EOF:
                    return make_token(Token::kEndOfInput);
                default:
                    return make_token(Token::kParseError);
            }
        }
    }

    [[nodiscard]] auto get_string() -> String
    {
        return move(m_scratch);
    }

    auto skip_comments() -> int
    {
        if (m_char != '/') {
            return 0; // Not a comment
        } else if (get() != '*') {
            return -1; // Parse error
        }
        for (;;) {
            switch (get()) {
                case '*':
                    if (get() == '/') {
                        return 0;
                    } else {
                        unget();
                    }
                    break;
                case '\0':
                    return -1;
                default:
                    break;
            }
        }
    }

    auto skip_whitespace() -> void
    {
        while (fast_isspace(peek())) {
            get();
        }
    }

    [[nodiscard]] auto make_token(Token::Kind kind, size_t size = 0) const -> Token
    {
        assert(size <= remaining() + 1);
        // Pointer to char stored in m_char is right before m_itr.
        const auto start = m_itr - 1;
        return {kind, start, start + size};
    }

    [[nodiscard]] auto make_error(const char *message) const -> Token
    {
        // `message` must be a string literal or nullptr.
        m_error_msg = message;
        return {Token::kParseError, nullptr, nullptr};
    }

    auto get(size_t n) -> const char *
    {
        if (remaining() < n) {
            return nullptr;
        }
        for (size_t i = 0; i < n && get(); ++i) {
            // Call get() so the position gets updated.
        }
        return m_itr - n;
    }

    auto get_hexcode() -> int
    {
        if (const auto *ptr = get(2)) {
            if (fast_ishex(ptr[0]) && fast_ishex(ptr[1])) {
                return fast_hexval(ptr[2]) << 4 |
                       fast_hexval(ptr[3]);
            }
        }
        return -1;
    }

    auto get_codepoint() -> int
    {
        if (const auto *ptr = get(4)) {
            if (fast_ishex(ptr[0]) && fast_ishex(ptr[1]) &&
                fast_ishex(ptr[2]) && fast_ishex(ptr[3])) {
                return fast_hexval(ptr[0]) << 12 |
                       fast_hexval(ptr[1]) << 8 |
                       fast_hexval(ptr[2]) << 4 |
                       fast_hexval(ptr[3]);
            }
        }
        return -1;
    }

    [[nodiscard]] auto scan_string() -> Token
    {
        StringBuilder sb;
        for (;;) {
            switch (get()) {
                case '\\':
                    switch (get()) {
                        case '\"':
                            sb.append('\"');
                            break;
                        case '\\':
                            sb.append('\\');
                            break;
                        case '/':
                            sb.append('/');
                            break;
                        case 'b':
                            sb.append('\b');
                            break;
                        case 'f':
                            sb.append('\f');
                            break;
                        case 'n':
                            sb.append('\n');
                            break;
                        case 'r':
                            sb.append('\r');
                            break;
                        case 't':
                            sb.append('\t');
                            break;
                        case 'u': {
                            auto codepoint = get_codepoint();
                            if (codepoint < 0) {
                                return make_error("missing 4 hex digits after `\\u`");
                            }
                            // TODO
                            break;
                        }
                        case 'x': {
                            const auto hexcode = get_hexcode();
                            if (hexcode < 0) {
                                return make_error("missing 2 hex digits after `\\x`");
                            }
                            sb.append(static_cast<char>(hexcode));
                            break;
                        }
                        default:
                            return make_error("unrecognized escape");
                    }
                    break;
                case '"':
                    if (sb.build(m_scratch)) {
                        return make_error(nullptr);
                    }
                    // Closing double quote finishes the string. Call get_string() to get the
                    // backing string buffer.
                    return Token{Token::kValueString, m_scratch.c_str(),
                                 m_scratch.c_str() + m_scratch.size()};
                case '\n':
                case '\0':
                case EOF:
                    return make_error("unexpected token");
                default:
                    sb.append(m_char);
            }
        }
    }

    Position m_pos = {};
    mutable const char *m_error_msg = nullptr;
    String m_scratch;
    const char *m_begin;
    const char *m_itr;
    const char *m_end;
    bool m_unget = false;
    char m_char;
};

} // namespace

auto DSLReader::register_event(EventType type, const Event &event) -> void
{
    m_events[type] = event;
}

auto DSLReader::read(const Slice &input, void *event_arg) -> Status
{
    Slice key_value[2];
    String backing[2];
    int depth = 0;

    enum State {
        kBeforeKey = 0,
        kAfterKey = 1,
        kBeforeValue = 2,
        kAfterValue = 3
    } state = kBeforeValue;
    Lexer lexer(input);

    const auto failure = [&lexer, &state](const char *message) {
        const auto &pos = lexer.position();
        const auto *msg = message;
        const auto *sep = ": ";
        if (!message) {
            sep = "";
            msg = "";
        }
        const char *exp;
        switch (state) {
            case kBeforeKey:
                exp = "key";
                break;
            case kAfterKey:
                exp = "':'";
                break;
            case kBeforeValue:
                exp = "value or '{'";
                break;
            case kAfterValue:
                exp = "',' or '}'";
                break;
        }
        return StatusBuilder::corruption("parse error at %d:%d%s%s (expected %s)",
                                         pos.line, pos.column, sep, msg, exp);
    };

    const auto dispatch = [=](auto type, const Slice *output) {
        m_events[type](event_arg, output);
    };

    Token token;
    Token last;
    for (;;) {
        last = token;
        token = lexer.scan();
        switch (token.kind) {
            case Token::kValueString:
                if (state & 1) {
                    return failure("expected value string");
                } else {
                    const auto idx = state / 2;
                    backing[idx] = lexer.get_string();
                    key_value[idx] = Slice(backing[idx].c_str(),
                                           backing[idx].size());
                    state = static_cast<State>(state + 1);
                }
                if (state == kAfterValue) {
                    dispatch(kReadKeyValue, key_value);
                }
                break;
            case Token::kBeginObject:
                static constexpr int kMaxDepth = 10'000;
                if (state != kBeforeValue) {
                    return failure("unexpected '{'");
                } else if (depth >= kMaxDepth) {
                    return failure("exceeded maximum object nesting");
                }
                dispatch(kBeginObject, key_value);
                state = kBeforeKey;
                ++depth;
                break;
            case Token::kEndObject:
                if (last.kind == Token::kBeginObject) {
                    // Object is empty.
                    state = kAfterValue;
                } else if (state != kAfterValue) {
                    return failure("unexpected '}'");
                } else if (depth <= 0) {
                    return failure("mismatched braces");
                }
                dispatch(kEndObject, nullptr);
                --depth;
                break;
            case Token::kNameSeparator:
                if (state != kAfterKey) {
                    return failure("unexpected ':'");
                }
                state = kBeforeValue;
                break;
            case Token::kValueSeparator:
                if (state != kAfterValue) {
                    return failure("unexpected ','");
                }
                state = kBeforeKey;
                break;
            case Token::kEndOfInput:
                if (state != kAfterValue || depth) {
                    return failure("incomplete structure");
                }
                return Status::ok();
            case Token::kParseError:
                return failure(lexer.error_msg());
        }
    }
}

} // namespace calicodb