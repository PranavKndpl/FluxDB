cmake_minimum_required(VERSION 3.15)
project(fluxdb_cpp_driver VERSION 1.0.0 LANGUAGES CXX)

add_library(fluxdb_driver INTERFACE)

target_include_directories(fluxdb_driver INTERFACE 
    ${CMAKE_CURRENT_SOURCE_DIR}
)

if(WIN32)
    target_link_libraries(fluxdb_driver INTERFACE ws2_32)
endif()

add_library(fluxdb::driver ALIAS fluxdb_driver)