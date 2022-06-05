
#include <filesystem>
#include "cub/cub.h"
#include "utils/utils.h"

using namespace cub;

static constexpr Size STRIDE = 8;
static constexpr Size INFO_SIZE = 1;
static constexpr Size KEY_SIZE = STRIDE/2 - INFO_SIZE;
static constexpr Size MAX_DATABASE_SIZE = 10'000;
static constexpr auto PATH = "/tmp/cub_fuzz";
static constexpr Options OPTIONS {
    0x200, // Database page size
    0x200, // WAL block size
    0x20,  // Number of buffer pool frames
    0666,  // File permissions
};

enum class Action {
    INSERT,
    REMOVE,
    COMMIT,
    ABORT,
};

static constexpr Action CHOICES[15] {
    Action::INSERT, Action::INSERT, Action::INSERT, Action::INSERT, Action::INSERT,
    Action::INSERT, Action::INSERT, Action::INSERT, Action::INSERT, Action::INSERT,
    Action::REMOVE, Action::REMOVE, Action::REMOVE,
    Action::COMMIT,
    Action::ABORT,
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, Size size)
{
    auto db = Database::open(PATH, OPTIONS);
    const auto info = db.get_info();

    if (info.record_count() > MAX_DATABASE_SIZE) {
        std::filesystem::remove(PATH);
        std::filesystem::remove(get_wal_path(PATH));
        return 0;
    }

    for (Index i {}; i + STRIDE <= size; i += STRIDE) {
        BytesView view {reinterpret_cast<const Byte*>(data + i*STRIDE), STRIDE};

        const Action action = CHOICES[*data >> 4];
        const Size multiplier = view[0] & 0x0F;
        view.advance();

        const auto key = view.range(0, KEY_SIZE);
        view.advance(KEY_SIZE);

        auto value = _s(view);
        for (Index m {}; m < multiplier; ++m)
            value += value;

        switch (action) {
            case Action::INSERT:
                db.insert(key, _b(value));
                break;
            case Action::REMOVE:
                // Remove the first record with a key >= `key`.
                if (const auto record = db.lookup(key, false))
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