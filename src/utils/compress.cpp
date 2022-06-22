//#ifndef CALICO_UTILS_CRC_H
//#define CALICO_UTILS_CRC_H
//
//#include "calico/bytes.h"
//#include <zlib.h>
//
//namespace calico {
//
//inline auto compress(Bytes dst, BytesView src) noexcept
//{
//    const auto *src_ptr = reinterpret_cast<const Bytef*>(src.data());
//    auto *dst_ptr = reinterpret_cast<Bytef*>(dst.data());
//    auto dst_len = static_cast<uLongf>(dst.size());
//    const auto rc = ::compress(dst_ptr, &dst_len, src_ptr, src.size());
//
//}
//
//inline auto decompress(Bytes dst, BytesView src) noexcept
//{
//    const auto ptr = reinterpret_cast<const Bytef*>(data.data());
//    const auto len = static_cast<unsigned>(data.size());
//    return static_cast<uint32_t>(crc32(0, ptr, len));
//}
//
//} // calico
//
//#endif // CALICO_UTILS_CRC_H
