# recursive files
# SET(CMAKE_BUILD_TYPE Release)
FILE(GLOB_RECURSE MINISQL_SOURCE ${PROJECT_SOURCE_DIR}/src/*/*.cpp
        ${PROJECT_SOURCE_DIR}/src/*/*/*.cpp
        ${PROJECT_SOURCE_DIR}/src/*/*.c
        ${PROJECT_SOURCE_DIR}/src/*/*/*.c
        )
ADD_LIBRARY(minisql_shared SHARED ${MINISQL_SOURCE})
TARGET_LINK_LIBRARIES(minisql_shared glog)
TARGET_LINK_LIBRARIES(minisql_shared dl)

ADD_EXECUTABLE(main main.cpp)
TARGET_LINK_LIBRARIES(main glog minisql_shared)