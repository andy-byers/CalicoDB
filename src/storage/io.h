#ifndef CALICO_STORAGE_IO_H
#define CALICO_STORAGE_IO_H

#include "interface.h"

namespace calico {

class File;

/**
 *
 * Warning: The file object used to create this object cannot be moved or destroyed while this object is live.
 */
class FileReader: public IFileReader {
public:
    ~FileReader() override = default;
    explicit FileReader(File&);
    auto seek(long, Seek) -> void override;
    auto read(Bytes) -> Size override;
    auto read_at(Bytes, Index) -> Size override;

    auto noex_seek(long, Seek) -> Result<Index> override;
    auto noex_read(Bytes) -> Result<Size> override;
    auto noex_read_at(Bytes, Index) -> Result<Size> override;

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
    auto seek(long, Seek) -> void override;
    auto write(BytesView) -> Size override;
    auto write_at(BytesView, Index) -> Size override;
    auto sync() -> void override;
    auto resize(Size) -> void override;

    auto noex_seek(long, Seek) -> Result<Index> override;
    auto noex_write(BytesView) -> Result<Size> override;
    auto noex_write_at(BytesView, Index) -> Result<Size> override;
    auto noex_sync() -> Result<void> override;
    auto noex_resize(Size) -> Result<void> override;

private:
    File *m_file {};
};

} // calico

#endif // CALICO_STORAGE_IO_H
