cmake_minimum_required(VERSION 3.25)
project(tree)

set(CMAKE_CXX_STANDARD 20)

add_executable(tree main.cpp node.cpp node.h encoding.h bplus_tree.cpp bplus_tree.h expect.h utils.h types.h ../pager/delta.cpp ../pager/delta.h header.cpp header.h ../pager/page.h include/calico/slice.h include/calico/common.h include/calico/status.h)
