#ifndef CALICO_TRANSACTION_H
#define CALICO_TRANSACTION_H

namespace Calico {

class Status;

class Transaction {
public:
    Transaction() = default;
    virtual ~Transaction();

    [[nodiscard]] virtual auto commit() -> Status = 0;
    [[nodiscard]] virtual auto abort() -> Status = 0;
};

} // namespace Calico

#endif // CALICO_TRANSACTION_H
