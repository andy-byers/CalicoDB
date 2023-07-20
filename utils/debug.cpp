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
    for (auto page_id = Id::root(); page_id.value <= pager.page_count(); ++page_id.value) {
        if (page_id.as_index() % 32 == 0) {
            os << SEP "|    PageID |  ParentID | PageType       | Info                            |\n" SEP;
        }
        Id parent_id;
        std::string info, type;
        if (PointerMap::is_map(page_id)) {
            const auto first = page_id.value + 1;
            append_fmt_string(info, "Range=[%u,%u]", first, first + kPageSize / 5 - 1);
            type = "<PtrMap>";
        } else {
            Status s;
            PointerMap::Entry entry;
            if (page_id.is_root()) {
                entry.type = PointerMap::kTreeRoot;
            } else {
                s = PointerMap::read_entry(pager, page_id, entry);
                if (!s.is_ok()) {
                    os << "error: " << s.to_string() << '\n';
                    return;
                }
                parent_id = entry.back_ptr;
            }
            PageRef *page;
            s = pager.acquire(page_id, page);
            if (!s.is_ok()) {
                os << "error: " << s.to_string() << '\n';
                return;
            }

            switch (entry.type) {
                case PointerMap::kTreeRoot:
                    type = "TreeRoot";
                    [[fallthrough]];
                case PointerMap::kTreeNode: {
                    auto n = NodeHdr::get_cell_count(
                        page->page + page_id.is_root() * FileHdr::kSize);
                    if (NodeHdr::get_type(page->page) == NodeHdr::kExternal) {
                        append_fmt_string(info, "Ex,N=%u,Sib=(%u,%u)", n,
                                          NodeHdr::get_prev_id(page->page).value,
                                          NodeHdr::get_next_id(page->page).value);
                    } else {
                        info = "In,N=";
                        append_number(info, n);
                        ++n;
                    }
                    if (type.empty()) {
                        type = "TreeNode";
                    }
                    break;
                }
                case PointerMap::kFreelistLeaf:
                    type = "Unused";
                    break;
                case PointerMap::kFreelistTrunk:
                    append_fmt_string(
                        info, "N=%u,Next=%u", get_u32(page->page + 4), get_u32(page->page));
                    type = "Freelist";
                    break;
                case PointerMap::kOverflowHead:
                    append_fmt_string(info, "Next=%u", get_u32(page->page));
                    type = "OvflHead";
                    break;
                case PointerMap::kOverflowLink:
                    append_fmt_string(info, "Next=%u", get_u32(page->page));
                    type = "OvflLink";
                    break;
                default:
                    type = "<BadType>";
            }
            pager.release(page);
        }
        std::string line;
        append_fmt_string(
            line,
            "|%10u |%10u | %-15s| %-32s|\n",
            page_id.value,
            parent_id.value,
            type.c_str(),
            info.c_str());
        os << line;
    }
    os << SEP;
#undef SEP
}

} // namespace calicodb

#endif // CALICODB_UTILS_DEBUG_H
