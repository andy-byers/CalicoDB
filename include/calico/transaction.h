#ifndef CALICO_TRANSACTION_H
#define CALICO_TRANSACTION_H

namespace calico {

class Core;
class Status;

/**
 * First-class representation of a transaction.
 *
 * The lifetime of this object is tied to that of a Calico DB transaction. The transaction begins when an instance is created, and ends when
 * either commit() or abort() is called, or the instance is destroyed. Each transaction object represents a single transaction. Once either
 * a commit() or an abort() call is successful, the instance can no longer be used.
 */
class Transaction final {
public:

    /**
     * Commit the transaction.
     *
     * @return A status object indicating success or failure.
     */
    [[nodiscard]] auto commit() -> Status;

    /**
     * Abort the transaction.
     *
     * @return A status object indicating success or failure.
     */
    [[nodiscard]] auto abort() -> Status;

    ~Transaction();
    explicit Transaction(Core&);

    Transaction(Transaction&&) noexcept;
    auto operator=(Transaction&&) noexcept -> Transaction&;

    Transaction(const Transaction&) = delete;
    auto operator=(const Transaction&) -> Transaction& = delete;

private:
    Core *m_core {}; ///< Pointer to the database that this object was opened on.
};

} // namespace calico

#endif // CALICO_TRANSACTION_H
