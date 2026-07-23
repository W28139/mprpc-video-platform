add_executable(user_consumer calluserservice.cc ../proto/user.pb.cc)
target_link_libraries(user_consumer mprpc protobuf)

add_executable(friend_consumer callfriendservice.cc ../proto/friend.pb.cc)
target_link_libraries(friend_consumer mprpc protobuf)
