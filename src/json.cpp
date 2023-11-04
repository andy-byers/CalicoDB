// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "json.h"
#include "buffer.h"
#include "internal.h"
#include "internal_vector.h"
#include "status_internal.h"

namespace calicodb::json
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
    kTokenError,
    kTokenCount
};

union Value {
    void *null = nullptr;
    bool boolean;
    int64_t integer;
    double real;
    Slice string;
};

class Accumulator
{
public:
    size_t total = 0;
    bool ok = true;

    explicit Accumulator(Buffer<char> &b)
        : m_buffer(&b)
    {
    }

    [[nodiscard]] auto result() const -> Slice
    {
        return m_buffer->is_empty() ? "" : Slice(m_buffer->data(), total);
    }

    void append(const Slice &data)
    {
        if (prepare_append(data.size())) {
            ok = false;
        } else {
            std::memcpy(m_buffer->data() + total, data.data(), data.size());
            total += data.size();
        }
    }

    void append(char c)
    {
        if (prepare_append(1)) {
            ok = false;
        } else {
            (*m_buffer)[total] = c;
            ++total;
        }
    }

private:
    [[nodiscard]] auto prepare_append(size_t extra_size) -> int
    {
        CALICODB_EXPECT_GE(m_buffer->size(), total);
        if (!ok) {
            return -1;
        }
        const auto needed_size = total + extra_size;
        if (needed_size > m_buffer->size()) {
            size_t capacity = 4;
            while (capacity < needed_size) {
                capacity *= 2;
            }
            if (m_buffer->resize(capacity)) {
                return -1;
            }
        }
        return 0;
    }

    Buffer<char> *const m_buffer;
};

class Lexer
{
public:
    struct Position {
        size_t line;
        size_t column;
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
        CALICODB_EXPECT_LE(m_itr, m_end);
        return m_itr >= m_end;
    }

    [[nodiscard]] auto remaining() const -> size_t
    {
        CALICODB_EXPECT_LE(m_itr, m_end);
        return static_cast<size_t>(m_end - m_itr);
    }

    [[nodiscard]] auto peek() const -> char
    {
        if (is_empty()) {
            return '\0';
        } else {
            return m_itr[0];
        }
    }

    auto get() -> char
    {
        if (is_empty()) {
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

    auto get4() -> const char *
    {
        if (remaining() < 4) {
            return nullptr;
        }
        get();
        get();
        get();
        get();
        return m_itr - 4;
    }

    void unget()
    {
        CALICODB_EXPECT_GE(m_itr, m_begin);
        --m_itr;
        m_char = m_itr == m_begin ? '\0' : m_itr[-1];
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
                    switch (get()) {
                        case '/':
                            return 0;
                        case '\0':
                            return -1;
                        default:
                            unget();
                    }
                    break;
                case '\0':
                    // End of input.
                    return -1;
                default:
                    break;
            }
        }
    }

    void skip_whitespace()
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
        return kTokenError;
    }

    auto get_codepoint() -> int
    {
        if (const auto *ptr = get4()) {
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
        Accumulator accum(m_scratch);
        for (;;) {
            switch (get()) {
                case '\\':
                    switch (get()) {
                        case '\"':
                            accum.append('\"');
                            break;
                        case '\\':
                            accum.append('\\');
                            break;
                        case '/':
                            accum.append('/');
                            break;
                        case 'b':
                            accum.append('\b');
                            break;
                        case 'f':
                            accum.append('\f');
                            break;
                        case 'n':
                            accum.append('\n');
                            break;
                        case 'r':
                            accum.append('\r');
                            break;
                        case 't':
                            accum.append('\t');
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
                                accum.append(static_cast<char>(codepoint));
                            } else if (codepoint <= 0x7FF) {
                                accum.append(static_cast<char>(0xC0 | ((codepoint >> 6) & 0xFF)));
                                accum.append(static_cast<char>(0x80 | ((codepoint & 0x3F))));
                            } else if (codepoint <= 0xFFFF) {
                                accum.append(static_cast<char>(0xE0 | ((codepoint >> 12) & 0xFF)));
                                accum.append(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
                                accum.append(static_cast<char>(0x80 | (codepoint & 0x3F)));
                            } else {
                                CALICODB_EXPECT_LE(codepoint, 0x10FFFF);
                                accum.append(static_cast<char>(0xF0 | ((codepoint >> 18) & 0xFF)));
                                accum.append(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
                                accum.append(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
                                accum.append(static_cast<char>(0x80 | (codepoint & 0x3F)));
                            }
                            break;
                        }
                        default:
                            return make_error("unrecognized escape");
                    }
                    break;
                case '"':
                    if (!accum.ok) {
                        return make_error(nullptr);
                    }
                    // Closing double quote finishes the string. Call get_string() to get the
                    // backing string buffer.
                    m_value.string = accum.result();
                    return kTokenValueString;
                default:
                    const auto c = static_cast<uint8_t>(m_char);
                    if (c >= 0x20) {
                        accum.append(m_char);
                    } else {
                        return make_error("unexpected token");
                    }
            }
        }
    }

    auto scan_number() -> Token
    {
        const auto negative = m_char == '-';
        const auto *begin = m_itr - 1;
        if (!negative) {
            unget();
        }

        if (peek() == 'e' || peek() == 'E') {
            // Catches cases like "-e2" and "-E5".
            return kTokenError;
        }
        if (get() == '0') {
            if (ISNUMERIC(peek())) {
                // '0' followed by another digit.
                return kTokenError;
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
            kFrac,   // Read a '.' to start "frac" part
            kDigits, // Read "frac" part digits
            kExp,    // Read an 'e' or 'E' to start "exp" part
            kSign,   // Read a '+' or '-' in "exp" part
            kPower,  // Read "exp" part digits
            kRealStateCount
        } state = kBegin;

        enum RealToken {
            kDot,   // .
            kE,     // e or E
            kPM,    // + or -
            k123,   // One of 0-9
            kOther, // None of the above
            kRealTokenCount
        };

        static constexpr RealState kTransitions[kRealStateCount][kRealTokenCount] = {
            //    Tokens = kDot,   kE,     kPM,    k123,   kOther
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
            state = kTransitions[state][token];
        } while (state > kBegin);

        if (state == kEnd) {
            char *end;
            m_value.real = strtod(begin, &end);
            m_itr = end;
            return kTokenValueReal;
        }
        return kTokenError;
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
        return kTokenError;
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
        return kTokenError;
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
        return kTokenError;
    }

    Position m_pos = {};
    Buffer<char> m_scratch;
    Value m_value;
    Status *const m_status;
    const char *const m_begin;
    const char *const m_end;
    const char *m_itr;
    char m_char = '\0';
};

class Parser
{
public:
    // This parser is a simple state machine with states defined by this enumerator. Nested structure
    // types are tracked using a bit vector. The general idea is from @Tencent/rapidjson.
    enum State {
        kStateEnd,
        kStateStop,
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
        : m_lex(input, m_status)
    {
    }

    using Callback = bool (*)(Handler &, Event, const Value &);

    struct Context {
        Handler *h;
        Callback cb;
    };

    auto parse(const Context &ctx) -> Status
    {
        Token token;
        State src = kStateBegin;
        while (advance(token)) {
            const auto dst = predict(src, token);
            src = transit(token, dst, ctx);
            if (src == kStateStop) {
                return m_status; // User-requested stop
            }
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
        const auto [line, column] = m_lex.get_position();
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
        return m_status.is_ok() && m_lex.scan(token);
    }

    // Predict the next state based on the current state and a token read by the lexer
    [[nodiscard]] auto predict(State src, Token token) -> State
    {
        // Note the comments to the right of each table row. The kStateBegin state (named "beg"
        // below) is marked "source" because it is the starting state. If each state is imagined
        // to be a vertex in a directed graph, and each state transition an edge, then kStateBegin
        // has no edges leading into it. Likewise, states marked "sink" have no edges leading out
        // of them (or more accurately, all edges lead to the error state). If a state is marked
        // "push", then we are entering a nested object or array. We need to remember what type of
        // structure we are currently in, so we push the current state onto a stack. A "pop" state
        // indicates that control is leaving a nested structure. The top of the stack is popped off
        // to reveal the type of structure the parser has just moved back into.
        // Note that all "pop" states are also sinks. This is because the parser needs information
        // contained in the stack in order to make a decision on what state to transition into.
        // The decision cannot be made in this function. It is the responsibility of transit() to
        // make sure the parser is transitioned to either kA1 or kO2, depending on the type of
        // structure that the parser has moved back into, so that additional values in that
        // structure can be parsed properly.
        static constexpr State kTransitions[kStateCount][kTokenCount] = {
#define end kStateEnd
#define ex_ kStateError
            // Token = "s"  123  1.0  T/F  nul   {    }    [    ]    :    ,   err
            /* end */ {ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_}, // sink
            /* stp */ {ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_, ex_}, // sink
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
    [[nodiscard]] auto transit(Token token, State dst, const Context &ctx) -> State
    {
        static constexpr int kMaxDepth = 10'000;
        auto emit_key = false; // Emit kEventKey if true
        // `dst` is the next state as predicted by predict(), upon reading token `token`.
        // We need to make sure that the destination state is not a transient state (either
        // kOE or kAE). As described in predict(), kOE (object end) and kAE (array end) are
        // "pop" states, meaning we pop an element off the stack and leave a nested structure.
        // After doing so, we examine the new stack top. Using that value, we transition to
        // either kO2 (object member value) or kA1 (array element). Normally, these states
        // are entered when we have just read an object member value or array element,
        // respectively, so basically, we are just treating the object or array as a child
        // member/element in its parent object/array.
        switch (dst) {
            case kV1:
            case kA1:
            case kO2:
                // Read a freestanding value, an array element, or an object member value.
                break;
            case kO1:
                // Special case for reading an object key.
                emit_key = true;
                break;
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
                // Save the current state on the stack.
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
        CALICODB_EXPECT_TRUE(emit_key || token < kTokenNameSeparator);
        const auto event = emit_key ? kEventKey : static_cast<Event>(token);
        if (!ctx.cb(*ctx.h, event, m_lex.get_value())) {
            return kStateStop;
        }
        return dst;
    }

    Status m_status;
    Vector<bool> m_stack;
    Lexer m_lex;
};

auto dispatch(Handler &handler, Event event, const Value &value) -> bool
{
    switch (event) {
        case kEventKey:
            return handler.accept_key(value.string);
        case kEventValueString:
            return handler.accept_string(value.string);
        case kEventValueInteger:
            return handler.accept_integer(value.integer);
        case kEventValueReal:
            return handler.accept_real(value.real);
        case kEventValueBoolean:
            return handler.accept_boolean(value.boolean);
        case kEventValueNull:
            return handler.accept_null();
        case kEventBeginObject:
            return handler.begin_object();
        case kEventEndObject:
            return handler.end_object();
        case kEventBeginArray:
            return handler.begin_array();
        default:
            return handler.end_array();
    }
}

} // namespace

Handler::Handler() = default;

Handler::~Handler() = default;

auto Reader::read(const Slice &input) -> Status
{
    return Parser(input)
        .parse({m_handler, dispatch});
}

} // namespace calicodb::json