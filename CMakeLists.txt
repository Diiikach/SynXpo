cmake_minimum_required(VERSION 4.0)

project(SynXpo VERSION 2.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# The previous synchronization implementation was intentionally removed.
# New transport, persistence, and application targets will be added here.
