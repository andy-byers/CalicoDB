#include "cub/cub.h"
#include "../tools/tools.h"

using namespace cub;

static constexpr Size INFO_SIZE {2};
static constexpr Size PAGE_SIZE {0x200};

enum class Operation {
    INSERT,
    REMOVE,
    COMMIT,
    ABORT,
};

static constexpr Operation CHOICES[0x10] {
    Operation::INSERT, Operation::INSERT, Operation::INSERT, Operation::INSERT, Operation::INSERT,
    Operation::INSERT, Operation::INSERT, Operation::INSERT, Operation::INSERT, Operation::INSERT,
    Operation::REMOVE, Operation::REMOVE, Operation::REMOVE, Operation::REMOVE,
    Operation::COMMIT,
    Operation::ABORT,
};

static constexpr auto CHARACTER_MAP =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "01234567";

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, Size size)
{
    auto db = Database::temp(PAGE_SIZE);
    Random random {0};

    for (Index i {}; i + INFO_SIZE <= size; ) {
        const Operation action = CHOICES[data[i] >> 4];
        const Size key_size = (data[i]&0x0F) + 1;
        const Size value_size = data[i + 1];
        const Size payload_size = key_size + value_size;
        i += INFO_SIZE;

        if (i + payload_size > size)
            break;

        std::string key(key_size, ' ');
        for (auto &c: key)
            c = CHARACTER_MAP[data[i++]];

        std::string value(value_size, ' ');
        for (auto &c: value)
            c = CHARACTER_MAP[data[i++]];

        switch (action) {
            case Operation::INSERT:
                db.insert(_b(key), _b(value));
                break;
            case Operation::REMOVE:
                // Remove the first record with a key >= `key`.
                if (const auto record = db.lookup(_b(key), false))
                    db.remove(_b(record->key));
                break;
            case Operation::COMMIT:
                db.commit();
                break;
            default:
                db.abort();
        }
    }
    return 0;
}