#ifndef CALICO_TRANSACTION_H
#define CALICO_TRANSACTION_H

namespace Calico {

class Core;
class Status;

class Transaction final {
public:
    ~Transaction();
    explicit Transaction(Core &core);

    // Prevent copies.
    Transaction(const Transaction &) = delete;
    auto operator=(const Transaction &) -> Transaction & = delete;

    Transaction(Transaction &&rhs) noexcept;
    auto operator=(Transaction &&rhs) noexcept -> Transaction &;

    [[nodiscard]] auto commit() -> Status;
    [[nodiscard]] auto abort() -> Status;

private:
    Core *m_core {};
};

} // namespace Calico

#endif // CALICO_TRANSACTION_H
