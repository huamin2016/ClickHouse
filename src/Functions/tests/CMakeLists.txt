#
CMAKE_MINIMUM_REQUIRED(VERSION 3.0.2)

INCLUDE_DIRECTORIES(3rd/poco/include)

project (funpro)

FUNCTION (SUBDIRS)
    FOREACH (dir ${ARGV})
        ADD_SUBDIRECTORY (${dir})
	message(${dir})
    ENDFOREACH(dir)
ENDFUNCTION (SUBDIRS)


message("1.PROJECT_BINARY_DIR = ${PROJECT_BINARY_DIR}")
message("2.PROJECT_SOURCE _DIR = ${PROJECT_SOURCE_DIR}")
message("3.CMAKE_CURRENT_SOURCE_DIR = ${CMAKE_CURRENT_SOURCE_DIR}")
message("4.CMAKE_CURRENT_BINARY_DIR = ${CMAKE_CURRENT_BINARY_DIR}")

#make gnlib
FILE (GLOB GNLIB_SRC gnlib.*)
add_library(gnlib STATIC ${GNLIB_SRC})

#必须在add前面,否则报找不到lib错误
link_directories(3rd/poco)

#
FILE (GLOB testsrc "${CMAKE_CURRENT_SOURCE_DIR}/test.cpp")
add_executable (funtest  ${testsrc})


#link 1
target_link_libraries(funtest gnlib poco)

#link 2
#当lib命令不是标准名称:lib[xxx].a时,需要写绝对路径
#target_link_libraries(funtest ${CMAKE_CURRENT_BINARY_DIR}/gnlib_v1.a)

