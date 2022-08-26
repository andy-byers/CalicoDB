#ifndef CALICO_TRANSACTION_H
#define CALICO_TRANSACTION_H

namespace calico {

class Core;
class Status;

class Transaction final {
public:
    [[nodiscard]] auto commit() -> Status;
    [[nodiscard]] auto abort() -> Status;

    ~Transaction();
    explicit Transaction(Core&);
    Transaction(Transaction&&) noexcept;
    auto operator=(Transaction&&) noexcept -> Transaction&;

    Transaction(const Transaction&) = delete;
    auto operator=(const Transaction&) -> Transaction& = delete;

private:
    Core *m_core {};
};

} // namespace calico

#endif // CALICO_TRANSACTION_H
