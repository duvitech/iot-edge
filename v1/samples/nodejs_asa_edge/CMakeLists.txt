#Copyright (c) Microsoft. All rights reserved.
#Licensed under the MIT license. See LICENSE file in the project root for full license information.

cmake_minimum_required(VERSION 2.8.12)

set(nodejs_asa_edge_sources
  ./src/main.c
  ${nodejs_asa_edge_json}
)
set(nodejs_asa_edge_headers
)

set_source_files_properties(${nodejs_asa_edge_json} PROPERTIES  HEADER_FILE_ONLY ON)

include_directories(
  ../../core/inc
  ${GW_INC}
)

set(LIBS ${LIBS} gateway)

add_executable(nodejs_asa_edge ${nodejs_asa_edge_headers} ${nodejs_asa_edge_sources})
target_link_libraries(nodejs_asa_edge ${LIBS})

# make nodejs_asa_edge depend on other modules
add_dependencies(nodejs_asa_edge nodejs_binding logger)

linkSharedUtil(nodejs_asa_edge)
install_broker(nodejs_asa_edge ${CMAKE_CURRENT_BINARY_DIR}/$(Configuration) )
copy_node_dll(nodejs_asa_edge ${CMAKE_CURRENT_BINARY_DIR}/$(Configuration) )
copy_gateway_dll(nodejs_asa_edge ${CMAKE_CURRENT_BINARY_DIR}/$(Configuration) )


add_sample_to_solution(nodejs_asa_edge)
