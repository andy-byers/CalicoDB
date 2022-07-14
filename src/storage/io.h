#ifndef CCO_STORAGE_IO_H
#define CCO_STORAGE_IO_H

#include "interface.h"

namespace cco {

class File;

/**
 *
 * Warning: The file object used to create this object cannot be moved or destroyed while this object is live.
 */
class FileReader: public IFileReader {
public:
    ~FileReader() override = default;
    explicit FileReader(File&);
    [[nodiscard]] auto seek(long, Seek) -> Result<Index> override;
    [[nodiscard]] auto read(Bytes) -> Result<Size> override;
    [[nodiscard]] auto read(Bytes, Index) -> Result<Size> override;

private:
    File *m_file {};
};

/**
 *
 * Warning: The file object used to create this object cannot be moved or destroyed while this object is live.
 */
class FileWriter: public IFileWriter {
public:
    ~FileWriter() override = default;
    explicit FileWriter(File&);
    [[nodiscard]] auto seek(long, Seek) -> Result<Index> override;
    [[nodiscard]] auto write(BytesView) -> Result<Size> override;
    [[nodiscard]] auto write(BytesView, Index) -> Result<Size> override;
    [[nodiscard]] auto sync() -> Result<void> override;
    [[nodiscard]] auto resize(Size) -> Result<void> override;

private:
    File *m_file {};
};

} // calico

#endif // CCO_STORAGE_IO_H
