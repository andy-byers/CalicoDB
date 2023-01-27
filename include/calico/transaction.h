#ifndef CALICO_TRANSACTION_H
#define CALICO_TRANSACTION_H

namespace Calico {

class DatabaseImpl;
class Status;

class Transaction final {
public:
    ~Transaction();

    // Prevent copies.
    Transaction(const Transaction &) = delete;
    auto operator=(const Transaction &) -> Transaction & = delete;

    Transaction(Transaction &&rhs) noexcept;
    auto operator=(Transaction &&rhs) noexcept -> Transaction &;

    [[nodiscard]] auto commit() -> Status;
    [[nodiscard]] auto abort() -> Status;

private:
    friend class DatabaseImpl;
    explicit Transaction(DatabaseImpl &impl);

    DatabaseImpl *m_impl {};
};

} // namespace Calico

#endif // CALICO_TRANSACTION_H
