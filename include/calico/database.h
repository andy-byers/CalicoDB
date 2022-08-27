#ifndef CALICO_DATABASE_H
#define CALICO_DATABASE_H

#include "bytes.h"
#include <memory>

namespace calico {

class Core;
class Cursor;
class Info;
class Status;
class Transaction;
struct Options;

/**
 * A Calico DB database!
 *
 * Note that any methods that modify the database are not thread safe. Multiple concurrent cursor objects are, however, allowed to operate as long
 * as no modifications take place while they are live.
 */
class Database final {
public:
    Database() noexcept;

    /**
     * Destroy a database.
     *
     * The database in question must be open.
     *
     * @param db An open database to destroy.
     * @return A status object indicating success or failure.
     */
    [[nodiscard]] static auto destroy(Database db) -> Status;

    /**
     * Open a database.
     *
     * If this method returns an OK status, the database will be considered open. Otherwise, calling any method besides this one will
     * result in undefined behavior.
     *
     * @param path Location of the database on disk.
     * @param options Initialization options for the database instance.
     * @return A status object indicating success or failure.
     */
    [[nodiscard]] auto open(const std::string &path, const Options &options = {}) -> Status;

    /**
     * Close a database.
     *
     * Once this method is called, the database will be considered closed, regardless of what is returned. Also, calling this method with a
     * transaction outstanding is undefined behavior.
     *
     * @return A status object indicating success or failure.
     */
    [[nodiscard]] auto close() -> Status;


    ///@{
    /**
     * Search for an exact key.
     *
     * @param key The key to search for.
     * @return A valid cursor positioned on the record with the given key, or an invalid cursor otherwise.
     */
    [[nodiscard]] auto find_exact(BytesView key) const -> Cursor;
    [[nodiscard]] auto find_exact(const std::string &key) const -> Cursor;
    ///@}

    ///@{
    /**
     * Search for the first record that is greater than or equal to the given key.
     *
     * @param key The key to search for against.
     * @return A cursor positioned on the first record greater than or equal to than the given key, or an
     *         invalid cursor if no such record exists.
     */
    [[nodiscard]] auto find(BytesView key) const -> Cursor;
    [[nodiscard]] auto find(const std::string &key) const -> Cursor;
    ///@}

    /**
     * Get a cursor positioned on the first record.
     *
     * @return A cursor positioned on the record with the smallest key, or an invalid cursor if the database is empty.
     */
    [[nodiscard]] auto first() const -> Cursor;

    /**
     * Get a cursor positioned on the last record.
     *
     * @return A cursor positioned on the record with the largest key, or an invalid cursor if the database is empty.
     */
    [[nodiscard]] auto last() const -> Cursor;

    ///@{
    /**
     * Insert a record.
     *
     * If the key already exists, this method overwrites the old value with the one provided. Note that calling this method will
     * invalidate any live cursors.
     */
    [[nodiscard]] auto insert(BytesView key, BytesView value) -> Status;
    [[nodiscard]] auto insert(const std::string &key, const std::string &value) -> Status;
    ///@}

    ///@{
    /**
     * Erase a record.
     *
     * Note that calling this method will invalidate any live cursors.
     *
     * @return A status object indicating success or failure. If the key does not exist, the status object will be in the "not found" state.
     */
    [[nodiscard]] auto erase(BytesView key) -> Status;
    [[nodiscard]] auto erase(const std::string &key) -> Status;
    [[nodiscard]] auto erase(const Cursor &cursor) -> Status;
    ///@}

    /**
     * Open an info object.
     *
     * @return An object that can be used to query information about the database.
     */
    [[nodiscard]] auto info() const -> Info;

    /**
     * Get the database status.
     *
     * If the status is not OK, then the database has encountered a fatal error during a modifying operation. If the error occurred during a
     * transaction, the transaction object can be used to fix the database state. If this is not possible, the program should be terminated and
     * recovery performed (automatically) on the next startup. If the error occurred outside of an explicit transaction, i.e. it is an atomic
     * operation, any changes will be automatically reverted, if possible, before the operation returns.
     *
     * @return The current database status value.
     */
    [[nodiscard]] auto status() const -> Status;

    /**
     * Open a transaction object.
     *
     * Only one transaction object may be live at any time. Once this method returns, the transaction has started.
     *
     * @return A first-class transaction object.
     */
    [[nodiscard]] auto transaction() -> Transaction;

    ~Database();

    // NOTE: Necessary because we have a non-trivial destructor.
    Database(Database&&) noexcept;
    Database& operator=(Database&&) noexcept;

private:
    std::unique_ptr<Core> m_core;
};

} // namespace calico

#endif // CALICO_DATABASE_H
