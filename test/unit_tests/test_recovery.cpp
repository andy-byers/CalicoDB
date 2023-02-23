/*
 * Recovery tests (harness is modified from LevelDB).
 */
#include "unit_tests.h"
#include "calico/calico.h"
#include "tools.h"

namespace Calico {

class RecoveryTests: public InMemoryTest {
public:
    RecoveryTests()  
        : db_prefix {"test/"}
    {
        db_options.storage = storage.get();
        open();
    }

    ~RecoveryTests() override
    {
        close();
    }

    auto impl() const -> DatabaseImpl * 
    {
        return reinterpret_cast<DatabaseImpl*>(db); 
    }
    
    void close() 
    {
        delete db;
        db = nullptr;
    }

    auto open_with_status(Options* options = nullptr)  -> Status
    {
        close();
        Options opts = db_options;
        if (options != nullptr) {
            opts = *options;
        }
        if (opts.storage == nullptr) {
            opts.storage = storage.get();
        }
        tail.resize(wal_block_size(opts.page_size));
        return Database::open(db_prefix, opts, &db);
    }

    auto open(Options* options = nullptr) -> void
    {
        ASSERT_OK(open_with_status(options));
    }

    auto put(const std::string& k, const std::string& v) -> Status 
    {
        return db->put(k, v);
    }

    auto get(const std::string& k) -> std::string
    {
        std::string result;
        Status s = db->get(k, result);
        if (s.is_not_found()) {
            result = "NOT_FOUND";
        } else if (!s.is_ok()) {
            result = s.what().to_string();
        }
        return result;
    }

    auto log_name(Id id) -> std::string
    {
        return encode_segment_name(db_prefix + "wal-", id);
    }

    auto remove_log_files() -> size_t
    {
        // Linux allows unlinking open files, but Windows does not.
        // Closing the db allows for file deletion.
        close();
        std::vector<Id> logs = get_logs();
        for (size_t i = 0; i < logs.size(); i++) {
            EXPECT_OK(storage->remove_file(encode_segment_name(db_prefix + "wal-", logs[i])));
        }
        return logs.size();
    }

    auto first_log_file() -> Id
    {
        return get_logs()[0];
    }

    auto get_logs() -> std::vector<Id>
    {
        std::vector<std::string> filenames;
        EXPECT_OK(storage->get_children(db_prefix, filenames));
        std::vector<Id> result;
        for (const auto &name: filenames) {
            if (name.find("wal-") != std::string::npos) {
                result.push_back(decode_segment_name("wal-", name));
            }
        }
        return result;
    }

    auto num_logs() -> Size
    { 
        return get_logs().size(); 
    }

    auto file_size(const std::string& fname) -> Size 
    {
        Size result;
        EXPECT_OK(storage->file_size(fname, result));
        return result;
    }

    // Write a commit record to a new segment.
    auto make_segment(Id lognum, Lsn seq) -> void
    {
        std::string fname = encode_segment_name(db_prefix + "wal-", lognum);
        Logger *file;
        ASSERT_OK(storage->new_logger(fname, &file));

        Byte commit_record[32] {};
        Span buffer {commit_record};
        const auto payload = encode_commit_payload(seq, buffer);

        WalWriter writer {*file, tail};
        ASSERT_OK(writer.write(encode_commit_payload(seq, buffer)));
        ASSERT_OK(writer.flush());
        delete file;
    }

    Tools::RandomGenerator random {1024 * 1024 * 4};
    Options db_options;
    std::string db_prefix;
    std::string tail;
    Database *db {};
};

TEST_F(RecoveryTests, NormalShutdown)
{
    ASSERT_OK(put("a", "1"));
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(put("c", "3"));
    ASSERT_OK(db->commit());
    close();

    ASSERT_EQ(num_logs(), 1);
}

TEST_F(RecoveryTests, OnlyCommittedUpdatesArePersisted)
{
    ASSERT_OK(put("a", "1"));
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(put("c", "3"));
    ASSERT_OK(db->commit());

    ASSERT_OK(put("c", "X"));
    ASSERT_OK(put("d", "4"));
    open();

    ASSERT_EQ(get("a"), "1");
    ASSERT_EQ(get("b"), "2");
    ASSERT_EQ(get("c"), "3");
    ASSERT_EQ(get("d"), "NOT_FOUND");
}

TEST_F(RecoveryTests, PacksMultipleTransactionsIntoSegment)
{
    ASSERT_OK(put("a", "1"));
    ASSERT_OK(db->commit());
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(db->commit());
    ASSERT_OK(put("c", "3"));
    ASSERT_OK(db->commit());

    ASSERT_EQ(num_logs(), 1);
    open();

    ASSERT_EQ(get("a"), "1");
    ASSERT_EQ(get("b"), "2");
    ASSERT_EQ(get("c"), "3");
}

TEST_F(RecoveryTests, RevertsNthTransaction)
{
    ASSERT_OK(put("a", "1"));
    ASSERT_OK(db->commit());
    ASSERT_OK(put("b", "2"));
    ASSERT_OK(db->commit());
    ASSERT_OK(put("c", "3"));
    open();

    ASSERT_EQ(get("a"), "1");
    ASSERT_EQ(get("b"), "2");
    ASSERT_EQ(get("c"), "NOT_FOUND");
}

}  // namespace Calico
