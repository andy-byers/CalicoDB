
#include "cub/cub.h"
#include "../tools/tools.h"

using namespace cub;

static constexpr Size STRIDE {8};
static constexpr Size INFO_SIZE {2};
static constexpr Size PAGE_SIZE {0x200};
static constexpr auto PATH = "/tmp/cub_fuzz";

enum class Action {
    INSERT,
    REMOVE,
    COMMIT,
    ABORT,
};

static constexpr Action CHOICES[0x10] {
    Action::INSERT, Action::INSERT, Action::INSERT, Action::INSERT, Action::INSERT,
    Action::INSERT, Action::INSERT, Action::INSERT, Action::INSERT, Action::INSERT,
    Action::REMOVE, Action::REMOVE, Action::REMOVE, Action::REMOVE,
    Action::COMMIT,
    Action::ABORT,
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

    for (Index i {}; i + 1 < size; ) {
        const Action action = CHOICES[data[i] >> 4];
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
            case Action::INSERT:
                db.insert(_b(key), _b(value));
                break;
            case Action::REMOVE:
                // Remove the first record with a key >= `key`.
                if (const auto record = db.lookup(_b(key), false))
                    db.remove(_b(record->key));
                break;
            case Action::COMMIT:
                db.commit();
                break;
            default:
                db.abort();
        }
    }
    return 0;
}

