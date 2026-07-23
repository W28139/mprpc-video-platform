set(SRC_LIST friendservice.cc ../proto/friend.pb.cc)
add_executable(provider ${SRC_LIST})
target_link_libraries(provider mprpc protobuf)

# set(SRC_LIST userservice.cc ../proto/user.pb.cc)
# add_executable(provider ${SRC_LIST})
# target_link_libraries(provider mprpc protobuf)
