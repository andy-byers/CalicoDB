// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "unit_tests.h"
#include "fake_env.h"

namespace calicodb
{

#define TRY_INTERCEPT_FROM(source, type, filename)                                                     \
    do {                                                                                               \
        if (auto intercept_s = (source).try_intercept_syscall(type, filename); !intercept_s.is_ok()) { \
            return intercept_s;                                                                        \
        }                                                                                              \
    } while (0)

template <class Container>
static auto try_intercept(const Container &interceptors, SyscallType type) -> Status
{
    Status s;
    for (const auto &interceptor : interceptors) {
        if (interceptor.type & type) {
            s = interceptor();
            if (!s.is_ok()) {
                break;
            }
        }
    }
    return s;
}

TestEnv::TestEnv()
    : EnvWrapper(*new FakeEnv)
{
}

TestEnv::TestEnv(Env &env)
    : EnvWrapper(env)
{
}

TestEnv::~TestEnv()
{
    if (target() != Env::default_env()) {
        delete target();
    }
}

auto TestEnv::save_file_contents(const std::string &filename) -> void
{
    std::lock_guard lock(m_mutex);
    auto state = m_state.find(filename);
    ASSERT_TRUE(state != end(m_state));
    state->second.saved_state = read_file_to_string(*target(), filename);
}

auto TestEnv::overwrite_file(const std::string &filename, const std::string &contents) -> void
{
    write_string_to_file(*target(), filename, contents, 0);
    ASSERT_OK(target()->resize_file(filename, contents.size()));
}

auto TestEnv::clone() -> Env *
{
    std::lock_guard lock(m_mutex);
    auto *env = new TestEnv(
        *reinterpret_cast<FakeEnv *>(target())->clone());
    for (const auto &state : m_state) {
        const auto file = read_file_to_string(*this, state.first);
        write_string_to_file(*env, state.first, file, 0);
    }
    return env;
}

auto TestEnv::drop_after_last_sync() -> void
{
    std::lock_guard lock(m_mutex);
    for (const auto &[filename, state] : m_state) {
        if (!state.unlinked) {
            overwrite_file(filename, state.saved_state);
        }
    }
}

auto TestEnv::drop_after_last_sync(const std::string &filename) -> void
{
    std::lock_guard lock(m_mutex);
    const auto state = m_state.find(filename);
    if (state != end(m_state) && !state->second.unlinked) {
        overwrite_file(filename, state->second.saved_state);
    }
}

auto TestEnv::find_counters(const std::string &filename) -> const FileCounters *
{
    std::lock_guard lock(m_mutex);
    auto itr = m_state.find(filename);
    if (itr != end(m_state)) {
        return &itr->second.counters;
    }
    return nullptr;
}

auto TestEnv::new_file(const std::string &filename, OpenMode mode, File *&out) -> Status
{
    TRY_INTERCEPT_FROM(*this, kSyscallOpen, filename);

    auto s = target()->new_file(filename, mode, out);
    if (s.is_ok()) {
        std::lock_guard lock(m_mutex);
        auto state = m_state.find(filename);
        if (state == end(m_state)) {
            state = m_state.insert(state, {filename, FileState()});
        }
        state->second.unlinked = false;
        out = new TestFile(filename, *out, *this, state->second);
    }
    return s;
}

auto TestEnv::resize_file(const std::string &filename, std::size_t file_size) -> Status
{
    TRY_INTERCEPT_FROM(*this, kSyscallResize, filename);
    return target()->resize_file(filename, file_size);
}

auto TestEnv::remove_file(const std::string &filename) -> Status
{
    TRY_INTERCEPT_FROM(*this, kSyscallUnlink, filename);

    auto s = target()->remove_file(filename);
    if (s.is_ok()) {
        auto state = m_state.find(filename);
        if (state != end(m_state)) {
            state->second.unlinked = true;
        } else {
            s = Status::io_error("no such file or directory");
        }
    }
    return s;
}

auto TestEnv::try_intercept_syscall(SyscallType type, const std::string &filename) -> Status
{
    std::lock_guard lock(m_mutex);
    const auto state = m_state.find(filename);
    if (state != end(m_state)) {
        // Need the position of the set bit to index the syscall counters array.
        std::size_t type_index = kNumSyscalls;
        for (std::size_t i = 0; i < kNumSyscalls; ++i) {
            if (type & (1 << i)) {
                type_index = i;
                break;
            }
        }
        CALICODB_EXPECT_LT(type_index, kNumSyscalls);
        ++state->second.counters.values[type_index];

        for (const auto &interceptor : state->second.interceptors) {
            if (interceptor.type & type) {
                return interceptor.callback();
            }
        }
    }
    return Status::ok();
}

auto TestEnv::copy_file(const std::string &source, const std::string &target) -> void
{
    (void)source;
    (void)target;
    std::size_t src_size = 0;
    CALICODB_EXPECT_TRUE(file_size(source, src_size).is_ok());

    File *src_file = nullptr;
    CALICODB_EXPECT_TRUE(new_file(source, kReadOnly, src_file).is_ok());
    File *dst_file = nullptr;
    CALICODB_EXPECT_TRUE(new_file(target, kCreate | kReadWrite, dst_file).is_ok());

    std::string buffer(src_size, '\0');
    CALICODB_EXPECT_TRUE(src_file->read_exact(0, src_size, buffer.data()).is_ok());
    CALICODB_EXPECT_TRUE(dst_file->write(0, buffer).is_ok());
    CALICODB_EXPECT_TRUE(dst_file->sync().is_ok());

    delete src_file;
    delete dst_file;
}

auto TestEnv::get_state(const std::string &filename) -> FileState &
{
    auto state = m_state.find(filename);
    if (state == end(m_state)) {
        state = m_state.insert(state, {filename, FileState()});
    }
    return state->second;
}

auto TestEnv::add_interceptor(const std::string &filename, Interceptor interceptor) -> void
{
    get_state(filename).interceptors.emplace_back(std::move(interceptor));
}

auto TestEnv::clear_interceptors() -> void
{
    for (auto &state : m_state) {
        state.second.interceptors.clear();
    }
}

auto TestEnv::clear_interceptors(const std::string &filename) -> void
{
    auto state = m_state.find(filename);
    if (state != end(m_state)) {
        state->second.interceptors.clear();
    }
}

TestFile::TestFile(std::string filename, File &file, TestEnv &env, TestEnv::FileState &state)
    : m_filename(std::move(filename)),
      m_env(&env),
      m_state(&state),
      m_target(&file)
{
}

TestFile::~TestFile()
{
    delete m_target;
}

auto TestFile::read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    auto s = try_intercept(m_state->interceptors, kSyscallRead);
    if (s.is_ok()) {
        s = m_target->read(offset, size, scratch, out);
    }
    return s;
}

auto TestFile::read_exact(std::size_t offset, std::size_t size, char *out) -> Status
{
    auto s = try_intercept(m_state->interceptors, kSyscallRead);
    if (s.is_ok()) {
        s = m_target->read_exact(offset, size, out);
    }
    return s;
}

auto TestFile::write(std::size_t offset, const Slice &in) -> Status
{
    auto s = try_intercept(m_state->interceptors, kSyscallWrite);
    if (s.is_ok()) {
        s = m_target->write(offset, in);
    }
    return s;
}

auto TestFile::sync() -> Status
{
    auto s = try_intercept(m_state->interceptors, kSyscallSync);
    if (s.is_ok()) {
        s = m_target->sync();
        if (s.is_ok()) {
            m_env->save_file_contents(m_filename);
        }
    } else {
        // Only drop data due to a non-OK status from an interceptor.
        m_env->drop_after_last_sync(m_filename);
    }
    return s;
}

auto TestFile::file_lock(FileLockMode mode) -> Status
{
    auto s = try_intercept(m_state->interceptors, kSyscallFileLock);
    if (s.is_ok()) {
        s = m_target->file_lock(mode);
    }
    return s;
}

auto TestFile::shm_map(std::size_t r, bool extend, volatile void *&ptr_out) -> Status
{
    auto s = try_intercept(m_state->interceptors, kSyscallShmMap);
    if (s.is_ok()) {
        s = m_target->shm_map(r, extend, ptr_out);
    }
    return s;
}

auto TestFile::shm_lock(std::size_t r, std::size_t n, ShmLockFlag flag) -> Status
{
    Status s;
    if (flag & kShmLock) {
        s = try_intercept(m_state->interceptors, kSyscallShmLock);
    }
    if (s.is_ok()) {
        s = m_target->shm_lock(r, n, flag);
    }
    return s;
}

auto print_database_overview(std::ostream &os, Pager &pager) -> void
{
#define SEP "|-----------|-----------|----------------|---------------------------------|\n"

    if (pager.page_count() == 0) {
        os << "DB is empty\n";
        return;
    }
    for (auto page_id = Id::root(); page_id.value <= pager.page_count(); ++page_id.value) {
        if (page_id.as_index() % 32 == 0) {
            os << SEP "|    PageID |  ParentID | PageType       | Info                            |\n" SEP;
        }
        Id parent_id;
        std::string info, type;
        if (PointerMap::is_map(page_id)) {
            const auto first = page_id.value + 1;
            append_fmt_string(info, "Range=[%u,%u]", first, first + kPageSize / 5 - 1);
            type = "<PtrMap>";
        } else {
            PointerMap::Entry entry;
            if (page_id.is_root()) {
                entry.type = PointerMap::kTreeRoot;
            } else {
                ASSERT_OK(PointerMap::read_entry(pager, page_id, entry));
                parent_id = entry.back_ptr;
            }
            Page page;
            ASSERT_OK(pager.acquire(page_id, page));

            switch (entry.type) {
                case PointerMap::kTreeRoot:
                    type = "TreeRoot";
                    [[fallthrough]];
                case PointerMap::kTreeNode: {
                    NodeHeader hdr;
                    hdr.read(page.constant_ptr() + page_id.is_root() * FileHeader::kSize);
                    auto n = hdr.cell_count;
                    if (hdr.is_external) {
                        append_fmt_string(info, "Ex,N=%u,Sib=(%u,%u)", n, hdr.prev_id.value, hdr.next_id.value);
                    } else {
                        info = "In,N=";
                        append_number(info, n);
                        ++n;
                    }
                    if (type.empty()) {
                        type = "TreeNode";
                    }
                    break;
                }
                case PointerMap::kFreelistLeaf:
                    type = "Unused";
                    break;
                case PointerMap::kFreelistTrunk:
                    append_fmt_string(
                        info, "N=%u,Next=%u", get_u32(page.constant_ptr() + 4), get_u32(page.constant_ptr()));
                    type = "Freelist";
                    break;
                case PointerMap::kOverflowHead:
                    append_fmt_string(info, "Next=%u", get_u32(page.constant_ptr()));
                    type = "OvflHead";
                    break;
                case PointerMap::kOverflowLink:
                    append_fmt_string(info, "Next=%u", get_u32(page.constant_ptr()));
                    type = "OvflLink";
                    break;
                default:
                    type = "<BadType>";
            }
            pager.release(std::move(page));
        }
        std::string line;
        append_fmt_string(
            line,
            "|%10u |%10u | %-15s| %-32s|\n",
            page_id.value,
            parent_id.value,
            type.c_str(),
            info.c_str());
        os << line;
    }
    os << SEP;
#undef SEP
}

#undef TRY_INTERCEPT_FROM

auto read_file_to_string(Env &env, const std::string &filename) -> std::string
{
    std::size_t file_size;
    const auto s = env.file_size(filename, file_size);
    if (s.is_io_error()) {
        // File was unlinked.
        return "";
    }
    std::string buffer(file_size, '\0');

    File *file;
    EXPECT_OK(env.new_file(filename, Env::kReadOnly, file));
    EXPECT_OK(file->read_exact(0, file_size, buffer.data()));
    delete file;

    return buffer;
}

auto write_string_to_file(Env &env, const std::string &filename, const std::string &buffer, long offset) -> void
{
    File *file;
    ASSERT_OK(env.new_file(filename, Env::kCreate, file));

    std::size_t write_pos;
    if (offset < 0) {
        ASSERT_OK(env.file_size(filename, write_pos));
    } else {
        write_pos = static_cast<std::size_t>(offset);
    }
    ASSERT_OK(file->write(write_pos, buffer));
    ASSERT_OK(file->sync());
    delete file;
}

auto assign_file_contents(Env &env, const std::string &filename, const std::string &contents) -> void
{
    ASSERT_OK(env.resize_file(filename, 0));
    write_string_to_file(env, filename, contents, 0);
}

auto hexdump_page(const Page &page) -> void
{
    std::fprintf(stderr, "%u:\n", page.id().value);
    for (std::size_t i = 0; i < kPageSize / 16; ++i) {
        std::fputs("    ", stderr);
        for (std::size_t j = 0; j < 16; ++j) {
            const auto c = page.constant_ptr()[i * 16 + j];
            if (std::isprint(c)) {
                std::fprintf(stderr, "%2c ", c);
            } else {
                std::fprintf(stderr, "%02X ", std::uint8_t(c));
            }
        }
        std::fputc('\n', stderr);
    }
}

auto fill_db(DB &db, const std::string &bname, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size) -> std::map<std::string, std::string>
{
    Tx *tx;
    EXPECT_OK(db.new_tx(WriteTag{}, tx));
    auto records = fill_db(*tx, bname, random, num_records, max_payload_size);
    EXPECT_OK(tx->commit());
    delete tx;
    return records;
}

auto fill_db(Tx &tx, const std::string &bname, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size) -> std::map<std::string, std::string>
{
    Bucket b;
    EXPECT_OK(tx.create_bucket(BucketOptions(), bname, &b));
    auto records = fill_db(tx, b, random, num_records, max_payload_size);
    return records;
}

auto fill_db(Tx &tx, const Bucket &b, RandomGenerator &random, std::size_t num_records, std::size_t max_payload_size) -> std::map<std::string, std::string>
{
    EXPECT_TRUE(max_payload_size > 0);
    std::map<std::string, std::string> records;

    for (std::size_t i = 0; i < num_records; ++i) {
        const auto ksize = random.Next(1, max_payload_size);
        const auto vsize = random.Next(max_payload_size - ksize);
        const auto k = random.Generate(ksize);
        const auto v = random.Generate(vsize);
        EXPECT_OK(tx.put(b, k, v));
        records[std::string(k)] = v;
    }
    return records;
}

auto expect_db_contains(DB &db, const std::string &bname, const std::map<std::string, std::string> &map) -> void
{
    const Tx *tx;
    ASSERT_OK(db.new_tx(tx));
    expect_db_contains(*tx, bname, map);
    delete tx;
}

auto expect_db_contains(const Tx &tx, const std::string &bname, const std::map<std::string, std::string> &map) -> void
{
    Bucket b;
    ASSERT_OK(tx.open_bucket(bname, b));
    expect_db_contains(tx, b, map);
}

auto expect_db_contains(const Tx &tx, const Bucket &b, const std::map<std::string, std::string> &map) -> void
{
    for (const auto &[key, value] : map) {
        std::string result;
        ASSERT_OK(tx.get(b, key, &result));
        ASSERT_EQ(result, value);
    }
}

FakeWal::FakeWal(const Parameters &param)
    : m_db_file(param.db_file)
{
}

auto FakeWal::read(Id page_id, char *&out) -> Status
{
    for (const auto &map : {m_pending, m_committed}) {
        const auto itr = map.find(page_id);
        if (itr != end(map)) {
            std::memcpy(out, itr->second.c_str(), kPageSize);
            return Status::ok();
        }
    }
    out = nullptr;
    return Status::ok();
}

auto FakeWal::write(PageRef *dirty, std::size_t db_size) -> Status
{
    for (auto *p = dirty; p; p = p->next) {
        m_pending.insert_or_assign(p->page_id, std::string(p->page, kPageSize));
    }
    if (db_size) {
        for (const auto &[k, v] : m_pending) {
            m_committed.insert_or_assign(k, v);
        }
        m_pending.clear();
        m_db_size = db_size;
    }
    return Status::ok();
}

auto FakeWal::checkpoint(bool) -> Status
{
    // TODO: Need the env to resize the file.
    CALICODB_EXPECT_TRUE(m_pending.empty());
    // Write back to the DB sequentially.
    for (const auto &[page_id, page] : m_committed) {
        const auto offset = page_id.as_index() * kPageSize;
        CALICODB_TRY(m_db_file->write(offset, page));
    }
    m_committed.clear();
    return Status::ok();
}

auto FakeWal::rollback(const Undo &undo) -> void
{
    for (const auto &[id, page] : m_pending) {
        undo(id);
    }
    m_pending.clear();
}

auto FakeWal::close() -> Status
{
    m_pending.clear();
    m_committed.clear();
    return Status::ok();
}

} // namespace calicodb

auto main(int argc, char **argv) -> int
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}