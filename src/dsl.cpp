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

#define ISSPACE(c) (kIsSpaceTable[static_cast<uint8_t>(c)])
#define ISHEX(c) (kIsHexTable[static_cast<uint8_t>(c)])
#define HEXVAL(c) (kIsHexTable[static_cast<uint8_t>(c)] - 1)

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
        CALICODB_EXPECT_FALSE(m_unget);
        CALICODB_EXPECT_NE(m_itr, m_begin);
        m_unget = true;
    }

    [[nodiscard]] auto scan() -> Token
    {
        skip_whitespace();
        while (m_char == '/') {
            if (skip_comments()) {
                return make_error("");
            }
            skip_whitespace();
        }

        for (;;) {
            switch (m_char) {
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
                    return make_token(Token::kEndOfInput);
                default:
                    return make_error("unexpected token");
            }
        }
    }

    [[nodiscard]] auto borrow_backing() -> String
    {
        return exchange(m_scratch, move(m_backup));
    }

    auto replace_backing(String str) -> void
    {
        if (!m_backup.is_empty()) {
            CALICODB_EXPECT_TRUE(m_scratch.is_empty());
            m_scratch = move(m_backup);
        }
        m_backup = move(str);
    }

    auto skip_comments() -> int
    {
        CALICODB_EXPECT_EQ(m_char, '/');
        if (get() != '*') {
            return -1; // Parse error
        }
        for (;;) {
            switch (get()) {
                case '*':
                    if (get() == '/') {
                        return 0;
                    }
                    unget();
                    break;
                case '\0':
                    // End of input.
                    return -1;
                default:
                    continue;
            }
        }
    }

    auto skip_whitespace() -> void
    {
        do {
            get();
        } while (ISSPACE(m_char));
    }

    [[nodiscard]] auto make_token(Token::Kind kind, size_t size = 0) const -> Token
    {
        // Already advanced past the char that was read into m_char, which is the start
        // of the token, hence the +/- 1 in this function.
        CALICODB_EXPECT_LE(size, remaining() + 1);
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
            if (ISHEX(ptr[0]) && ISHEX(ptr[1])) {
                return HEXVAL(ptr[0]) << 4 |
                       HEXVAL(ptr[1]);
            }
        }
        return -1;
    }

    auto get_codepoint() -> int
    {
        if (const auto *ptr = get(4)) {
            if (ISHEX(ptr[0]) && ISHEX(ptr[1]) &&
                ISHEX(ptr[2]) && ISHEX(ptr[3])) {
                return HEXVAL(ptr[0]) << 12 |
                       HEXVAL(ptr[1]) << 8 |
                       HEXVAL(ptr[2]) << 4 |
                       HEXVAL(ptr[3]);
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
                            if (0xD800 <= codepoint && codepoint <= 0xDFFF) {
                                // Codepoint is part of a surrogate pair. Expect a high surrogate (U+D800–U+DBFF) followed
                                // by a low surrogate (U+DC00–U+DFFF).
                                if (codepoint <= 0xDBFF) {
                                    if (get() != '\\' || get() != 'u') {
                                        return make_error("missing low surrogate");
                                    }
                                    const auto codepoint2 = get_codepoint();
                                    if (codepoint2 < 0xDC00 || codepoint2 > 0xDFFF) {
                                        return make_error("low surrogate is malformed");
                                    }
                                    codepoint = (((codepoint - 0xD800) << 10) |
                                                 (codepoint2 - 0xDC00)) +
                                                0x10000;
                                } else {
                                    return make_error("missing high surrogate");
                                }
                            }
                            // Translate the codepoint into bytes. Modified from @Tencent/rapidjson.
                            if (codepoint <= 0x7F) {
                                sb.append(static_cast<char>(codepoint));
                            } else if (codepoint <= 0x7FF) {
                                sb.append(static_cast<char>(0xC0 | ((codepoint >> 6) & 0xFF)));
                                sb.append(static_cast<char>(0x80 | ((codepoint & 0x3F))));
                            } else if (codepoint <= 0xFFFF) {
                                sb.append(static_cast<char>(0xE0 | ((codepoint >> 12) & 0xFF)));
                                sb.append(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
                                sb.append(static_cast<char>(0x80 | (codepoint & 0x3F)));
                            } else {
                                CALICODB_EXPECT_LE(codepoint, 0x10FFFF);
                                sb.append(static_cast<char>(0xF0 | ((codepoint >> 18) & 0xFF)));
                                sb.append(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
                                sb.append(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
                                sb.append(static_cast<char>(0x80 | (codepoint & 0x3F)));
                            }
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
                default:
                    const auto c = static_cast<uint8_t>(m_char);
                    if (c >= 0x20) {
                        sb.append(m_char);
                    } else {
                        return make_error("unexpected token");
                    }
            }
        }
    }

    Position m_pos = {};
    mutable const char *m_error_msg = nullptr;
    String m_scratch;
    String m_backup;
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
    // This parser is a simple state machine with states described by the State enum. The
    // kBefore* states indicate that the parser is expecting a token of type *. The word "Value"
    // in the k*Value states refers to either a string value or a nested object. The following
    // state transitions are possible:
    //
    //  Before      | Token::Kind     | After
    // -------------|-----------------|--------------
    // kBeforeKey   | kValueString    | kAfterKey
    // kAfterKey    | kNameSeparator  | kBeforeValue
    // kBeforeValue | kBeginObject    | kBeforeKey
    // kBeforeValue | kValueString    | kAfterValue
    // kAfterValue  | kValueSeparator | kBeforeKey
    enum State {
        kBeforeKey = 0,
        kAfterKey = 1,
        kBeforeValue = 2,
        kAfterValue = 3
    } state = kBeforeValue;
    Lexer lexer(input);

    const auto failure = [&lexer, &state](const char *message) {
        CALICODB_EXPECT_TRUE(message);
        const auto [line, column] = lexer.position();
        const char *expected;
        switch (state) {
            case kBeforeKey:
                expected = "key";
                break;
            case kAfterKey:
                expected = "':'";
                break;
            case kBeforeValue:
                expected = "value or '{'";
                break;
            case kAfterValue:
                expected = "',' or '}'";
                break;
        }
        return StatusBuilder::corruption("parse error at %d:%d%s (expected %s)",
                                         line, column, message, expected);
    };

    const auto dispatch = [=](auto type, const Slice *output) {
        if (m_events[type]) {
            m_events[type](event_arg, output);
        }
    };

    int depth = 0;
    Slice key_value[2];
    String backing[2];
    Token::Kind last;
    Token token = {};

    // Parse loop. Lexer::scan() is called once at the start of each iteration. The "last"
    // variable is used to detect empty objects.
    for (;;) {
        last = token.kind;
        token = lexer.scan();
        switch (token.kind) {
            case Token::kValueString:
                if (state & 1) {
                    return failure("expected value string");
                } else {
                    const auto idx = state / 2;
                    backing[idx] = lexer.borrow_backing();
                    key_value[idx] = Slice(backing[idx].c_str(),
                                           backing[idx].size());
                    state = static_cast<State>(state + 1);
                }
                if (state == kAfterValue) {
                    dispatch(kReadKeyValue, key_value);
                    // A string key and value are stored in the backing strings. Give them back to
                    // the lexer so that they can be reused.
                    lexer.replace_backing(move(backing[0]));
                    lexer.replace_backing(move(backing[1]));
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
                // backing[0] contains the key string. It is not needed after the event finishes.
                lexer.replace_backing(move(backing[0]));
                state = kBeforeKey;
                ++depth;
                break;
            case Token::kEndObject:
                if (last == Token::kBeginObject) {
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