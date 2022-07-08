#ifndef CALICO_EXCEPTION_H
#define CALICO_EXCEPTION_H

#include <cerrno>
#include <cstring>
#include <exception>
#include <system_error>
#include <utility>
#include "options.h"

namespace calico {

/**
 * An exception that is thrown when corruption is detected.
 */
class CorruptionError: public std::runtime_error {
public:
    explicit CorruptionError(const std::string &what)
        : std::runtime_error {what} {}

    ~CorruptionError() override = default;
};

} // calico

#endif // CALICO_EXCEPTION_H
