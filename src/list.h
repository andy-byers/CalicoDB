// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_LIST_H
#define CALICODB_LIST_H

namespace calicodb
{

struct IntrusiveList {
    IntrusiveList() = delete;

    template <class Entry>
    [[nodiscard]] static auto is_empty(const Entry &entry) -> bool
    {
        return &entry == entry.next_entry;
    }

    template <class Entry>
    static void initialize(Entry &entry)
    {
        entry.prev_entry = &entry;
        entry.next_entry = &entry;
    }

    template <class Entry>
    static void add_between(Entry &entry, Entry &prev, Entry &next)
    {
        next.prev_entry = &entry;
        entry.next_entry = &next;
        entry.prev_entry = &prev;
        prev.next_entry = &entry;
    }

    template <class Entry>
    static void add_head(Entry &ref, Entry &head)
    {
        IntrusiveList::add_between(ref, head, *head.next_entry);
    }

    template <class Entry>
    static void add_tail(Entry &entry, Entry &head)
    {
        IntrusiveList::add_between(entry, *head.prev_entry, head);
    }

    template <class Entry>
    static void remove(Entry &entry)
    {
        entry.next_entry->prev_entry = entry.prev_entry;
        entry.prev_entry->next_entry = entry.next_entry;
    }
};

} // namespace calicodb

#endif // CALICODB_LIST_H
