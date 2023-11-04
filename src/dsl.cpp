// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "dsl.h"
#include "internal.h"
#include "internal_vector.h" // TODO: Write a wrapper that uses a single bit per bool, instead of a whole byte
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

constexpr uint8_t kIsNumericTable[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0, 0,
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
#define ISNUMERIC(c) (kIsNumericTable[static_cast<uint8_t>(c)])
#define NUMVAL(c) (kIsNumericTable[static_cast<uint8_t>(c)] - 1)
#define ISHEX(c) (kIsHexTable[static_cast<uint8_t>(c)])
#define HEXVAL(c) (kIsHexTable[static_cast<uint8_t>(c)] - 1)

enum Token {
    kTokenValueString = kEventValueString,
    kTokenValueInteger = kEventValueInteger,
    kTokenValueReal = kEventValueReal,
    kTokenValueBoolean = kEventValueBoolean,
    kTokenValueNull = kEventValueNull,
    kTokenBeginObject = kEventBeginObject,
    kTokenEndObject = kEventEndObject,
    kTokenBeginArray = kEventBeginArray,
    kTokenEndArray = kEventEndArray,
    kTokenNameSeparator,
    kTokenValueSeparator,
    kTokenParseError,
    kTokenCount
};

class Lexer
{
public:
    struct Position {
        int line;
        int column;
    };

    explicit Lexer(const Slice &input, Status &status)
        : m_value{nullptr},
          m_status(&status),
          m_begin(input.data()),
          m_end(m_begin + input.size()),
          m_itr(m_begin)
    {
    }

    [[nodiscard]] auto get_value() const -> const Value &
    {
        return m_value;
    }

    [[nodiscard]] auto get_position() const -> const Position &
    {
        return m_pos;
    }

    [[nodiscard]] auto scan(Token &token) -> bool
    {
        CALICODB_EXPECT_TRUE(m_status->is_ok());
        skip_whitespace();
        while (m_char == '/') {
            if (skip_comments()) {
                token = make_error("");
                return false;
            }
            skip_whitespace();
        }

        for (;;) {
            switch (m_char) {
                case '"':
                    token = scan_string();
                    break;
                case '-':
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    token = scan_number();
                    break;
                case 'n':
                    token = scan_null();
                    break;
                case 't':
                    token = scan_true();
                    break;
                case 'f':
                    token = scan_false();
                    break;
                case ':':
                    token = kTokenNameSeparator;
                    break;
                case ',':
                    token = kTokenValueSeparator;
                    break;
                case '{':
                    token = kTokenBeginObject;
                    break;
                case '}':
                    token = kTokenEndObject;
                    break;
                case '[':
                    token = kTokenBeginArray;
                    break;
                case ']':
                    token = kTokenEndArray;
                    break;
                default:
                    token = make_error("unexpected character");
                    [[fallthrough]];
                case '\0':
                    return false;
            }
            return true;
        }
    }

private:
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
        return is_empty() ? '\0' : m_itr[-m_unget];
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

    auto expect(char c) -> bool
    {
        if (peek() == c) {
            get();
            return true;
        }
        return false;
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

    [[nodiscard]] auto make_error(const char *message) const -> Token
    {
        if (message) {
            *m_status = Status::corruption(message);
        } else {
            *m_status = Status::no_memory();
        }
        return kTokenParseError;
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
        // Reuse the scratch buffer. Overwrite it starting at offset 0.
        StringBuilder sb(move(m_scratch), 0);
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
                    m_value.string = Slice(m_scratch);
                    return kTokenValueString;
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

    auto scan_number() -> Token
    {
        const auto negative = m_itr[-1] == '-';
        const auto *begin = m_itr - 1;
        if (!negative) {
            unget();
        }

        if (peek() == 'e' || peek() == 'E') {
            // Catches cases like "-e2" and "-E5".
            return kTokenParseError;
        }
        if (get() == '0') {
            if (ISNUMERIC(peek())) {
                // '0' followed by another digit.
                return kTokenParseError;
            }
        }
        unget();

        int64_t value = 0;
        while (ISNUMERIC(peek())) {
            const unsigned v = NUMVAL(get());
            const auto is_last = !ISNUMERIC(peek());
            if (value > INT64_MAX / 10) {
                // This number is definitely too large to be represented as an int64_t. Parse
                // it as a double instead.
                goto call_scan_real;
            } else if (value == INT64_MAX / 10) {
                // This number might be too large. Handle the special cases (modified from
                // SQLite). Note that INT64_MAX is equal to 9223372036854775807. In this
                // branch, v is referring to the least-significant digit of INT64_MAX, since
                // the value * 10 below will yield 9223372036854775800.
                if (v == 9 || !is_last) {
                    goto call_scan_real;
                } else if (v == 8) {
                    if (negative && is_last) {
                        m_value.integer = INT64_MIN;
                        return kTokenValueInteger;
                    }
                    goto call_scan_real;
                }
            }
            value = value * 10 + v;
        }

        switch (peek()) {
            case 'e':
            case 'E':
            case '.':
                break;
            default:
                m_value.integer = negative ? -value : value;
                return kTokenValueInteger;
        }

    call_scan_real:
        return scan_real(begin);
    }

    auto scan_real(const char *begin) -> Token
    {
        // According to RFC 8259, the ABNF for a number looks like:
        //     [ minus ] int [ frac ] [ exp ]
        // We have already parsed the "[ minus ]" and "int" parts. Either the "int" part overflowed, or there
        // exists a "frac" or "exp" part that needs to be scanned. Since parsing floating-point numbers is
        // extremely complicated, we are using strtod(). All we have to do is make sure the rest of the number
        // is formatted correctly.
        enum RealState {
            kEnd,
            kError,
            kBegin,
            kFrac,   // Read a '.'
            kDigits, // Read "frac" part digits
            kExp,    // Read an 'e' or 'E'
            kSign,   // Read a '+' or '-' in kExp
            kPower,  // Read "exp" part digits
            kRealStateCount
        } phase = kBegin;

        enum RealToken {
            kDot,   // .
            kE,     // e or E
            kPM,    // + or -
            k123,   // One of 0-9
            kOther, // None of the above
            kRealTokenCount
        };

        static constexpr RealState kTransitions[kRealStateCount][kRealTokenCount] = {
            //               kDot      kE     kPM    k123   kOther
            /*    kEnd */ {kError, kError, kError, kError, kError}, // Sink
            /*  kError */ {kError, kError, kError, kError, kError}, // Sink
            /*  kBegin */ {kFrac, kExp, kError, kError, kEnd},      // Source
            /*    kDot */ {kError, kError, kError, kDigits, kError},
            /* kDigits */ {kError, kExp, kError, kDigits, kEnd},
            /*      kE */ {kError, kError, kSign, kPower, kError},
            /*   kSign */ {kError, kError, kError, kPower, kError},
            /*  kPower */ {kError, kError, kError, kPower, kEnd},
        };

        // If an integer overflowed, then there might be some digits left. The code below
        // expects just the fractional or exponential parts, so skip any remaining digits
        // in the integral part.
        while (ISNUMERIC(peek())) {
            get();
        }

        // Validate the rest of the number.
        do {
            RealToken token;
            switch (get()) {
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    // Leading 0s are allowed in both the "frac" and "exp" parts.
                    token = k123;
                    break;
                case '.':
                    token = kDot;
                    break;
                case 'e':
                case 'E':
                    token = kE;
                    break;
                case '+':
                case '-':
                    token = kPM;
                    break;
                default:
                    token = kOther;
            }
            phase = kTransitions[phase][token];
        } while (phase > kBegin);

        if (phase == kEnd) {
            char *end;
            m_value.real = strtod(begin, &end);
            m_itr = end;
            return kTokenValueReal;
        }
        return kTokenParseError;
    }

    auto scan_null() -> Token
    {
        CALICODB_EXPECT_EQ(m_char, 'n');
        if (get() == 'u' &&
            get() == 'l' &&
            get() == 'l') {
            m_value.null = nullptr;
            return kTokenValueNull;
        }
        return kTokenParseError;
    }

    auto scan_true() -> Token
    {
        CALICODB_EXPECT_EQ(m_char, 't');
        if (get() == 'r' &&
            get() == 'u' &&
            get() == 'e') {
            m_value.boolean = true;
            return kTokenValueBoolean;
        }
        return kTokenParseError;
    }

    auto scan_false() -> Token
    {
        CALICODB_EXPECT_EQ(m_char, 'f');
        if (get() == 'a' &&
            get() == 'l' &&
            get() == 's' &&
            get() == 'e') {
            m_value.boolean = false;
            return kTokenValueBoolean;
        }
        return kTokenParseError;
    }

    Position m_pos = {};
    String m_scratch;
    Value m_value;
    Status *const m_status;
    const char *const m_begin;
    const char *const m_end;
    const char *m_itr;
    bool m_unget = false;
    char m_char;
};

class Parser
{
public:
    // This parser is a simple state machine with states defined by this enumerator. Nested structure
    // types are tracked using a bit vector. The general idea is from @Tencent/rapidjson.
    enum State {
        kStateEnd,
        kStateError,
        kStateBegin,
        kAB, // Array begin
        kA1, // Array element
        kAx, // Array element separator
        kAE, // Array end
        kOB, // Object begin
        kO1, // Object key
        kOx, // Object key separator
        kO2, // Object value
        kOy, // Object value separator
        kOE, // Object end
        kV1, // Freestanding value
        kStateCount
    };

    explicit Parser(const Slice &input)
        : m_lexer(input, m_status)
    {
    }

    template <class Dispatch>
    auto parse(const Dispatch &dispatch) -> Status
    {
        Token token;
        State src = kStateBegin;
        while (advance(token)) {
            const auto dst = predict(src, token);
            src = transit(token, dst, dispatch);
        }
        return finish(src);
    }

private:
    auto out_of_memory() -> State
    {
        CALICODB_EXPECT_TRUE(m_status.is_ok());
        m_status = Status::no_memory();
        return kStateError;
    }

    auto corruption(const char *message) -> State
    {
        CALICODB_EXPECT_TRUE(m_status.is_ok());
        const auto [line, column] = m_lexer.get_position();
        m_status = StatusBuilder::corruption("corruption detected at %d:%d (%s)",
                                             line, column, message);
        return kStateError;
    }

    auto finish(State state) -> Status
    {
        if (!m_status.is_ok()) {
            return m_status;
        }
        if (!m_stack.is_empty()) {
            corruption("structure was not closed");
        } else if (state != kStateEnd &&
                   state != kV1 &&
                   state != kA1 &&
                   state != kO2) {
            corruption("incomplete or missing structure");
        }
        return m_status;
    }

    [[nodiscard]] auto advance(Token &token) -> bool
    {
        return m_status.is_ok() && m_lexer.scan(token);
    }

    // Predict the next state based on the current state and a token read by the lexer
    [[nodiscard]] auto predict(State src, Token token) -> State
    {
        // Note the comments to the right of each table row. The kStateBegin state (named "beg"
        // below) is marked "Source" because it is the starting state. If each state is imagined
        // to be a vertex in a directed graph, and each state transition an edge, then kStateBegin
        // has no edges leading into it. Likewise, states marked "Sink" have no edges leading out
        // of them. If a state is marked "Push", then we are entering a nested object or array.
        // We need to remember what type of structure we are currently in, so we push the current
        // state onto a stack. A "Pop" state indicates that control is leaving a nested structure.
        // The top of the stack is popped off to reveal the type of structure the parser has just
        // moved back into. The parser needs to make sure it doesn't end up in a pop state at the
        // end of the iteration. It is the responsibility of transit() to make sure the parser is
        // transitioned to either kA1 or kO2, depending on the value that is now at the top of the
        // stack, so that further values can be parsed.
        static constexpr State kTransitions[kStateCount][kTokenCount] = {
#define end kStateEnd
#define ex_ kStateError
            // Token = "s"  123  1.0  T/F  nul   {    }    [    ]    :    ,   err
            /* end */ {ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_}, // sink
            /* ex_ */ {ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_}, // sink
            /* beg */ {kV1, kV1, kV1, kV1, kV1, kOB, ex_, kAB, ex_, ex_, ex_, ex_}, // source
            /* kAB */ {kA1, kA1, kA1, kA1, kA1, kOB, ex_, kAB, kAE, ex_, ex_, ex_}, // push
            /* kA1 */ {ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, kAE, ex_, kAx, ex_},
            /* kAx */ {kA1, kA1, kA1, kA1, kA1, kOB, ex_, kAB, ex_, ex_, ex_, ex_},
            /* kAE */ {ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_}, // pop
            /* kOB */ {kO1, ex_, ex_, ex_, ex_, ex_, kOE, ex_, ex_, ex_, ex_, ex_}, // push
            /* kO1 */ {ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, kOx, ex_, ex_},
            /* kOx */ {kO2, kO2, kO2, kO2, kO2, kOB, ex_, kAB, ex_, ex_, ex_, ex_},
            /* kO2 */ {ex_, ex_, ex_, ex_, ex_, ex_, kOE, ex_, ex_, ex_, kOy, ex_},
            /* kOy */ {kO1, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_},
            /* kOE */ {ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_}, // pop
            /* kV1 */ {ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_}, // sink
#undef end
#undef ex_
        };
        return kTransitions[src][token];
    }

    // Transition into the next state
    template <class Dispatch>
    [[nodiscard]] auto transit(Token token, State dst, const Dispatch &dispatch) -> State
    {
        static constexpr int kMaxDepth = 10'000;
        const Value *value = nullptr;
        switch (dst) {
            case kV1:
            case kA1:
            case kO2:
                // Read a freestanding value, an array element, or an object member value.
                value = &m_lexer.get_value();
                break;
            case kO1:
                // Special case for reading an object key.
                dispatch(kEventKey, &m_lexer.get_value());
                return dst;
            case kAx:
            case kOx:
            case kOy:
                // Transition states return immediately without dispatching an event.
                return dst;
            case kAB:
            case kOB:
                // Opened a new array or object.
                if (m_stack.size() == kMaxDepth) {
                    return corruption("exceeded maximum structure depth");
                }
                if (m_stack.push_back(dst == kOB)) {
                    return out_of_memory();
                }
                break;
            case kAE:
            case kOE:
                // Closed an array or object. The stack is never empty here, since we immediately transition
                // to the end state if we remove the last element below. Also, we can only get to this state
                // if we have pushed onto the stack at least once (reading a freestanding value leads to
                // state kV1, which is a sink state).
                CALICODB_EXPECT_FALSE(m_stack.is_empty());
                m_stack.pop_back();
                if (m_stack.is_empty()) {
                    dst = kStateEnd; // Must be finished
                } else {
                    dst = m_stack.back() ? kO2 : kA1; // After value
                }
                break;
            case kStateError:
                if (m_status.is_ok()) {
                    return corruption("fail");
                }
                [[fallthrough]];
            default: // kStateEnd | kStateError
                return dst;
        }
        CALICODB_EXPECT_LT(token, kTokenNameSeparator);
        dispatch(static_cast<Event>(token), value);
        return dst;
    }

    Status m_status;
    Vector<bool> m_stack;
    Lexer m_lexer;
};

} // namespace

auto DSLReader::register_action(Event event, const Action &action) -> void
{
    m_actions[event] = action;
}

auto DSLReader::dispatch(Event event, void *action_arg, const Value *value) -> void
{
    if (m_actions[event]) {
        m_actions[event](action_arg, value);
    }
}

auto DSLReader::read(const Slice &input, void *action_arg) -> Status
{
    return Parser(input).parse(
        [this, action_arg](Event event, const Value *value) {
            dispatch(event, action_arg, value);
        });
}

} // namespace calicodb