#aux_source_directory(. SRC_LIST)
set(SRC_LIST
            mprpcapplication.cc
            mprpcconfig.cc
            rpcheader.pb.cc
            rpcprovider.cc
            mprpcchannel.cc
            mprpccontroller.cc
            ZookeeperUtil.cc
            mprpcredis.cc
            mprpcmetrics.cc)
add_library(mprpc ${SRC_LIST})
target_include_directories(mprpc PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(mprpc wevix_muduo pthread zookeeper_mt hiredis)