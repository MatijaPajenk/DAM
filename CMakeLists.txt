cmake_minimum_required(VERSION 3.25)

project(DAM LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(DAM src/main.cpp)

target_compile_options(DAM PRIVATE
    -Wall
    -Wextra
    -Wpedantic
)