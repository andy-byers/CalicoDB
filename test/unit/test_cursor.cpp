//
// Created by Andrew Byers on 5/20/22.
//


////class CursorTestSuite: public TreeTests {
////public:
////    CursorTestSuite() = default;
////    ~CursorTestSuite() override = default;
////
////    auto initialize_sanity_check(Size num_entries) -> void
////    {
////        // Even keys in [2, 2*num_entries].
////        m_num_entries = num_entries;
////        auto builder = TreeBuilder{tree()};
////        m_keys.resize(num_entries);
////        std::iota(m_keys.begin(), m_keys.end(), min_key() / KEY_STEP);
////        for (const auto &key: keys)
////            builder.tree_insert(make_key(key * KEY_STEP));
////        ASSERT_EQ(keys.front() * KEY_STEP, min_key());
////        ASSERT_EQ(keys.back() * KEY_STEP, max_key());
////    }
////
////    auto initialize_with_keys(const std::vector<Index> &keys) -> void
////    {
////        m_keys = keys;
////        auto builder = TreeBuilder{tree()};
////        for (const auto &key: keys)
////            builder.tree_insert(make_key(key));
////    }
////
////    auto key(Index position) const -> Index
////    {
////        return m_keys.at(position);
////    }
////
////    auto min_key() const -> Index
////    {
////        CUB_EXPECT_GT(m_num_entries, 0);
////        return m_min_key;
////    }
////
////    auto max_key() const -> Index
////    {
////        CUB_EXPECT_GT(m_num_entries, 0);
////        return m_max_key;
////    }
////
////    std::vector<Index> m_keys;
////    Index m_min_key{};
////    Index m_max_key{};
////};
////
////TEST_F(CursorTestSuite, HandlesEmptyTrees)
////{
////    auto cursor = tree().open_cursor(false);
////
////    const auto check = [&cursor]() {
////        return !cursor->has_entry() && !cursor->decrement() && !cursor->increment();
////    };
////
////    ASSERT_TRUE(check());
////
////    // Traversal operations can still be performed but will do nothing.
////    cursor->find_local_min();
////    ASSERT_TRUE(check());
////    cursor->find_local_max();
////    ASSERT_TRUE(check());
////    ASSERT_FALSE(cursor->find(to_bytes(make_key(0))));
////    ASSERT_TRUE(check());
////}
////
////TEST_F(CursorTestSuite, CursorStaysInRange)
////{
////    initialize_sanity_check(0x100);
////    auto cursor = tree().open_cursor(false);
////    cursor->find_local_min();
////    ASSERT_FALSE(cursor->can_decrement());
////    ASSERT_FALSE(cursor->decrement());
////    ASSERT_EQ(to_string(cursor->key()), make_key(min_key()));
////
////    cursor->reset();
////    cursor->find_local_max();
////    ASSERT_FALSE(cursor->can_increment());
////    ASSERT_FALSE(cursor->increment());
////    ASSERT_EQ(to_string(cursor->key()), make_key(max_key()));
////}
////
////TEST_F(CursorTestSuite, FindsExistentEntries)
////{
////    initialize_sanity_check(0x100);
////    auto cursor = tree().open_cursor(false);
////
////    for (auto i = min_key(); i <= max_key(); i += KEY_STEP) {
////        ASSERT_TRUE(cursor->find(to_bytes(make_key(i))));
////        ASSERT_EQ(to_string(cursor->key()), make_key(i));
////        cursor->reset();
////    }
////}
////
////TEST_F(CursorTestSuite, DoesNotFindNonExistentEntries)
////{
////    initialize_sanity_check(0x100);
////    ASSERT_GT(min_key(), 0);
////
////    auto cursor = tree().open_cursor(false);
////    for (auto i = min_key() - 1; i < max_key(); i += KEY_STEP) {
////        ASSERT_FALSE(cursor->find(to_bytes(make_key(i))));
////        ASSERT_EQ(to_string(cursor->key()), make_key(i + 1));
////        cursor->reset();
////    }
////}
////
////class TraversalTestSuite: public CursorTestSuite {
////public:
////
////};
////
////auto run_traversal_test(CursorTestSuite &test, Size num_entries, Index start, Size count, bool is_forward) -> void
////{
////    constexpr auto step = CursorTestSuite::KEY_STEP;
////    ASSERT_LE(start + count, num_entries);
////
////    ASSERT_TRUE(!is_forward || start + count <= num_entries);
////    ASSERT_TRUE(is_forward || start >= count);
////
////    test.initialize_sanity_check(num_entries);
////    auto key = test.min_key() + start*step;
////    auto cursor = test.tree().open_cursor(false);
////    ASSERT_TRUE(cursor->find(to_bytes(make_key(key))));
////
////    for (Index i{}; i < count; ++i) {
////        ASSERT_EQ(to_string(cursor->key()), make_key(key));
////
////        if (is_forward) {
////            cursor->increment();
////            key += step;
////        } else {
////            cursor->decrement();
////            key -= step;
////        }
////    }
////}
////
////TEST_F(CursorTestSuite, FullForwardTraversal)
////{
////    run_traversal_test(*this, 100, 0, 100, true);
////}
////
////TEST_F(CursorTestSuite, FullReverseTraversal)
////{
////    run_traversal_test(*this, 100, 99, 100, false);
////}
////
////TEST_F(CursorTestSuite, PartialForwardTraversal)
////{
////    run_traversal_test(*this, 100, 20, 60, true);
////}
////
////TEST_F(CursorTestSuite, PartialReverseTraversal)
////{
////    run_traversal_test(*this, 100, 20, 60, true);
////}
//
//
////TEST_F(CursorTestSuite, FullReverseTraversal)
////{
////    //                                         1:[08]
////    //           7:[02,          05]                          8:[11,          14]
////    // 2:[00, 01]      3:[03, 04]   4:[06, 07]      5:[09, 10]      6:[12, 13]   9:[15, 16]
////    TreeBuilder builder{tree()};
////    setup_cursor_test(builder);
////    auto cursor = tree().open_cursor(true);
////    ASSERT_TRUE(cursor->find(to_bytes(make_key(16))));
////    for (Index i{}; i < 16; ++i) {
////        ASSERT_EQ(to_string(cursor->key()), make_key(16 - i));
////        cursor->decrement();
////    }
////    ASSERT_EQ(to_string(cursor->key()), make_key(0));
////}
////
////TEST_F(CursorTestSuite, PartialForwardTraversal)
////{
////    initialize_sanity_check(0x100);
////    const auto start = min_key() + 10*KEY_STEP;
////    const auto finish = max_key() - 10*KEY_STEP;
////    auto cursor = tree().open_cursor(false);
////    ASSERT_TRUE(cursor->find(to_bytes(make_key(start))));
////    auto counter = start;
////    while (counter < finish) {
////        ASSERT_EQ(to_string(cursor->key()), make_key(counter));
////        cursor->increment();
////        counter += KEY_STEP;
////    }
////    ASSERT_EQ(to_string(cursor->key()), make_key(finish));
////}
////
////TEST_F(CursorTestSuite, PartialReverseTraversal)
////{
////    initialize_sanity_check(0x100);
////    const auto start = max_key() - 10*KEY_STEP;
////    const auto finish = min_key() + 10*KEY_STEP;
////    auto cursor = tree().open_cursor(false);
////    ASSERT_TRUE(cursor->find(to_bytes(make_key(start))));
////    auto counter = start;
////    while (counter > finish) {
////        ASSERT_EQ(to_string(cursor->key()), make_key(counter));
////        cursor->decrement();
////        counter -= KEY_STEP;
////    }
////    ASSERT_EQ(to_string(cursor->key()), make_key(finish));
////}
//
////TEST_F(CursorTestSuite, CursorSanityCheck)
////{
////    Random random{0};
////    TreeBuilder builder{tree()};
////    for (Index i{}; i < 150; ++i)
////        builder.tree_insert(make_key(i));
////    for (Index iteration{}; iteration < 20; ++iteration) {
////        const auto lower_int = random.next_int(0UL, 100UL);
////        const auto upper_int = random.next_int(lower_int, 150UL);
////        const auto lower = make_key(lower_int);
////        const auto upper = make_key(upper_int);
////        auto cursor = tree().open_cursor_at(to_bytes(lower), false);
////        auto counter = lower_int;
////        while (compare_bytes_three_way(cursor->key(), to_bytes(upper)) == ThreeWayComparison::LT) {
////            ASSERT_EQ(to_string(cursor->key()), make_key(counter));
////            cursor->increment();
////            counter++;
////        }
////    }
////}