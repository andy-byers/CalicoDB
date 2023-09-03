// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_UTILS_DEBUG_H
#define CALICODB_UTILS_DEBUG_H

#include "common.h"
#include "logging.h"
#include "pager.h"
#include <iostream>

namespace calicodb
{

auto print_database_overview(std::ostream &os, Pager &pager) -> void
{
#define SEP "|-----------|-----------|----------------|---------------------------------|\n"

    if (pager.page_count() == 0) {
        os << "DB is empty\n";
        return;
    }
    auto &root = pager.get_root();
    auto s = FileHdr::check_db_support(root.data);
    if (s.is_ok()) {
        os << "File Header:\n";
        os << "    | Field           | Value\n";
        os << "    |-----------------|-------\n";
        os << "    | page_count      | " << FileHdr::get_page_count(root.data) << '\n';
        os << "    | freelist_head   | " << FileHdr::get_freelist_head(root.data).value << '\n';
        os << "    | freelist_length | " << FileHdr::get_freelist_length(root.data) << '\n';
        os << "    | largest_root    | " << FileHdr::get_largest_root(root.data).value << '\n';
        os << "    | page_size       | " << FileHdr::get_page_size(root.data) << '\n';
        os << "Root Header:\n";
        os << "    | Field           | Value\n";
        os << "    |-----------------|-------\n";
        os << "    | type       | " << NodeHdr::get_type(root.data + FileHdr::kSize) << '\n';
        os << "    | cell_count | " << NodeHdr::get_cell_count(root.data + FileHdr::kSize) << '\n';
        os << "    | cell_start | " << NodeHdr::get_cell_start(root.data + FileHdr::kSize) << '\n';
        os << "    | free_start | " << NodeHdr::get_free_start(root.data + FileHdr::kSize) << '\n';
        os << "    | frag_count | " << NodeHdr::get_frag_count(root.data + FileHdr::kSize) << '\n';
        os << "    | next_id    | " << NodeHdr::get_next_id(root.data + FileHdr::kSize).value << '\n';
    }
    for (auto page_id = Id::root(); page_id.value <= pager.page_count(); ++page_id.value) {
        if (page_id.as_index() % 32 == 0) {
            os << SEP "|    PageID |  ParentID | PageType       | Info                            |\n" SEP;
        }
        Id parent_id;
        String info, type;
        if (PointerMap::is_map(page_id, pager.page_size())) {
            const auto first = page_id.value + 1;
            (void)append_format_string(info, "Range=[%u,%u]", first, first + pager.page_size() / 5 - 1);
            (void)append_strings(type, "<PtrMap>");
        } else {
            PointerMap::Entry entry;
            if (page_id.is_root()) {
                entry.type = PointerMap::kTreeRoot;
            } else {
                s = PointerMap::read_entry(pager, page_id, entry);
                if (!s.is_ok()) {
                    os << "error: " << s.message() << '\n';
                    return;
                }
                parent_id = entry.back_ptr;
            }
            PageRef *page;
            s = pager.acquire(page_id, page);
            if (!s.is_ok()) {
                os << "error: " << s.message() << '\n';
                return;
            }

            switch (entry.type) {
                case PointerMap::kTreeRoot:
                    (void)append_strings(type, "TreeRoot");
                    [[fallthrough]];
                case PointerMap::kTreeNode: {
                    auto n = NodeHdr::get_cell_count(
                        page->data + page_id.is_root() * FileHdr::kSize);
                    if (NodeHdr::get_type(page->data) == NodeHdr::kExternal) {
                        (void)append_format_string(info, "Ex,N=%u", n);
                    } else {
                        (void)append_strings(type, "In,N=", std::to_string(n));
                        ++n;
                    }
                    if (type.is_empty()) {
                        (void)append_strings(type, "TreeNode");
                    }
                    break;
                }
                case PointerMap::kFreelistPage:
                    (void)append_strings(type, "Freelist");
                    break;
                case PointerMap::kOverflowHead:
                    (void)append_format_string(info, "Next=%u", get_u32(page->data));
                    (void)append_strings(type, "OvflHead");
                    break;
                case PointerMap::kOverflowLink:
                    (void)append_format_string(info, "Next=%u", get_u32(page->data));
                    (void)append_strings(type, "OvflLink");
                    break;
                default:
                    (void)append_strings(type, "<BadType>");
            }
            pager.release(page);
        }
        String line;
        (void)append_format_string(
            line,
            "|%10u |%10u | %-15s| %-32s|\n",
            page_id.value,
            parent_id.value,
            type.c_str(),
            info.c_str());
        os << line.c_str();
    }
    os << SEP;
#undef SEP
}

} // namespace calicodb

#endif // CALICODB_UTILS_DEBUG_H
