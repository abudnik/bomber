cmake_minimum_required(VERSION 2.6)
set(TARGET bomber)
set(CMAKE_CXX_FLAGS "-Wall -pedantic -g -std=c++0x -O2")

find_package(Boost REQUIRED COMPONENTS iostreams thread regex program_options system filesystem)

set(REQUIRED_LIBRARIES
  elliptics_cpp
  elliptics_client
  ${Boost_LIBRARIES}
)

set(SOURCE
  ${PROJECT_SOURCE_DIR}/bomber.cpp
)

add_executable(${TARGET} ${SOURCE})
target_link_libraries(${TARGET} ${REQUIRED_LIBRARIES})
