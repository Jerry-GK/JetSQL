# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.23

# Default target executed when no arguments are given to make.
default_target: all
.PHONY : default_target

# Allow only one "make -f Makefile2" at a time, but pass parallelism.
.NOTPARALLEL:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /Applications/CMake.app/Contents/bin/cmake

# The command to remove a file.
RM = /Applications/CMake.app/Contents/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /Users/jerryliterm/codes/MiniSQL

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /Users/jerryliterm/codes/MiniSQL

#=============================================================================
# Targets provided globally by CMake.

# Special rule for the target package
package: preinstall
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --cyan "Run CPack packaging tool..."
	/Applications/CMake.app/Contents/bin/cpack --config ./CPackConfig.cmake
.PHONY : package

# Special rule for the target package
package/fast: package
.PHONY : package/fast

# Special rule for the target package_source
package_source:
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --cyan "Run CPack packaging tool for source..."
	/Applications/CMake.app/Contents/bin/cpack --config ./CPackSourceConfig.cmake /Users/jerryliterm/codes/MiniSQL/CPackSourceConfig.cmake
.PHONY : package_source

# Special rule for the target package_source
package_source/fast: package_source
.PHONY : package_source/fast

# Special rule for the target test
test:
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --cyan "Running tests..."
	/Applications/CMake.app/Contents/bin/ctest --force-new-ctest-process $(ARGS)
.PHONY : test

# Special rule for the target test
test/fast: test
.PHONY : test/fast

# Special rule for the target edit_cache
edit_cache:
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --cyan "Running CMake cache editor..."
	/Applications/CMake.app/Contents/bin/ccmake -S$(CMAKE_SOURCE_DIR) -B$(CMAKE_BINARY_DIR)
.PHONY : edit_cache

# Special rule for the target edit_cache
edit_cache/fast: edit_cache
.PHONY : edit_cache/fast

# Special rule for the target rebuild_cache
rebuild_cache:
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --cyan "Running CMake to regenerate build system..."
	/Applications/CMake.app/Contents/bin/cmake --regenerate-during-build -S$(CMAKE_SOURCE_DIR) -B$(CMAKE_BINARY_DIR)
.PHONY : rebuild_cache

# Special rule for the target rebuild_cache
rebuild_cache/fast: rebuild_cache
.PHONY : rebuild_cache/fast

# Special rule for the target list_install_components
list_install_components:
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --cyan "Available install components are: \"Development\" \"Unspecified\""
.PHONY : list_install_components

# Special rule for the target list_install_components
list_install_components/fast: list_install_components
.PHONY : list_install_components/fast

# Special rule for the target install
install: preinstall
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --cyan "Install the project..."
	/Applications/CMake.app/Contents/bin/cmake -P cmake_install.cmake
.PHONY : install

# Special rule for the target install
install/fast: preinstall/fast
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --cyan "Install the project..."
	/Applications/CMake.app/Contents/bin/cmake -P cmake_install.cmake
.PHONY : install/fast

# Special rule for the target install/local
install/local: preinstall
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --cyan "Installing only the local directory..."
	/Applications/CMake.app/Contents/bin/cmake -DCMAKE_INSTALL_LOCAL_ONLY=1 -P cmake_install.cmake
.PHONY : install/local

# Special rule for the target install/local
install/local/fast: preinstall/fast
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --cyan "Installing only the local directory..."
	/Applications/CMake.app/Contents/bin/cmake -DCMAKE_INSTALL_LOCAL_ONLY=1 -P cmake_install.cmake
.PHONY : install/local/fast

# Special rule for the target install/strip
install/strip: preinstall
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --cyan "Installing the project stripped..."
	/Applications/CMake.app/Contents/bin/cmake -DCMAKE_INSTALL_DO_STRIP=1 -P cmake_install.cmake
.PHONY : install/strip

# Special rule for the target install/strip
install/strip/fast: preinstall/fast
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --cyan "Installing the project stripped..."
	/Applications/CMake.app/Contents/bin/cmake -DCMAKE_INSTALL_DO_STRIP=1 -P cmake_install.cmake
.PHONY : install/strip/fast

# The main all target
all: cmake_check_build_system
	$(CMAKE_COMMAND) -E cmake_progress_start /Users/jerryliterm/codes/MiniSQL/CMakeFiles /Users/jerryliterm/codes/MiniSQL//CMakeFiles/progress.marks
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 all
	$(CMAKE_COMMAND) -E cmake_progress_start /Users/jerryliterm/codes/MiniSQL/CMakeFiles 0
.PHONY : all

# The main clean target
clean:
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 clean
.PHONY : clean

# The main clean target
clean/fast: clean
.PHONY : clean/fast

# Prepare targets for installation.
preinstall: all
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 preinstall
.PHONY : preinstall

# Prepare targets for installation.
preinstall/fast:
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 preinstall
.PHONY : preinstall/fast

# clear depends
depend:
	$(CMAKE_COMMAND) -S$(CMAKE_SOURCE_DIR) -B$(CMAKE_BINARY_DIR) --check-build-system CMakeFiles/Makefile.cmake 1
.PHONY : depend

#=============================================================================
# Target rules for targets named gtest

# Build rule for target.
gtest: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 gtest
.PHONY : gtest

# fast build rule for target.
gtest/fast:
	$(MAKE) $(MAKESILENT) -f googletest-build/CMakeFiles/gtest.dir/build.make googletest-build/CMakeFiles/gtest.dir/build
.PHONY : gtest/fast

#=============================================================================
# Target rules for targets named gtest_main

# Build rule for target.
gtest_main: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 gtest_main
.PHONY : gtest_main

# fast build rule for target.
gtest_main/fast:
	$(MAKE) $(MAKESILENT) -f googletest-build/CMakeFiles/gtest_main.dir/build.make googletest-build/CMakeFiles/gtest_main.dir/build
.PHONY : gtest_main/fast

#=============================================================================
# Target rules for targets named Experimental

# Build rule for target.
Experimental: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 Experimental
.PHONY : Experimental

# fast build rule for target.
Experimental/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/Experimental.dir/build.make glog-build/CMakeFiles/Experimental.dir/build
.PHONY : Experimental/fast

#=============================================================================
# Target rules for targets named Nightly

# Build rule for target.
Nightly: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 Nightly
.PHONY : Nightly

# fast build rule for target.
Nightly/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/Nightly.dir/build.make glog-build/CMakeFiles/Nightly.dir/build
.PHONY : Nightly/fast

#=============================================================================
# Target rules for targets named Continuous

# Build rule for target.
Continuous: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 Continuous
.PHONY : Continuous

# fast build rule for target.
Continuous/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/Continuous.dir/build.make glog-build/CMakeFiles/Continuous.dir/build
.PHONY : Continuous/fast

#=============================================================================
# Target rules for targets named NightlyMemoryCheck

# Build rule for target.
NightlyMemoryCheck: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 NightlyMemoryCheck
.PHONY : NightlyMemoryCheck

# fast build rule for target.
NightlyMemoryCheck/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/NightlyMemoryCheck.dir/build.make glog-build/CMakeFiles/NightlyMemoryCheck.dir/build
.PHONY : NightlyMemoryCheck/fast

#=============================================================================
# Target rules for targets named NightlyStart

# Build rule for target.
NightlyStart: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 NightlyStart
.PHONY : NightlyStart

# fast build rule for target.
NightlyStart/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/NightlyStart.dir/build.make glog-build/CMakeFiles/NightlyStart.dir/build
.PHONY : NightlyStart/fast

#=============================================================================
# Target rules for targets named NightlyUpdate

# Build rule for target.
NightlyUpdate: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 NightlyUpdate
.PHONY : NightlyUpdate

# fast build rule for target.
NightlyUpdate/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/NightlyUpdate.dir/build.make glog-build/CMakeFiles/NightlyUpdate.dir/build
.PHONY : NightlyUpdate/fast

#=============================================================================
# Target rules for targets named NightlyConfigure

# Build rule for target.
NightlyConfigure: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 NightlyConfigure
.PHONY : NightlyConfigure

# fast build rule for target.
NightlyConfigure/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/NightlyConfigure.dir/build.make glog-build/CMakeFiles/NightlyConfigure.dir/build
.PHONY : NightlyConfigure/fast

#=============================================================================
# Target rules for targets named NightlyBuild

# Build rule for target.
NightlyBuild: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 NightlyBuild
.PHONY : NightlyBuild

# fast build rule for target.
NightlyBuild/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/NightlyBuild.dir/build.make glog-build/CMakeFiles/NightlyBuild.dir/build
.PHONY : NightlyBuild/fast

#=============================================================================
# Target rules for targets named NightlyTest

# Build rule for target.
NightlyTest: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 NightlyTest
.PHONY : NightlyTest

# fast build rule for target.
NightlyTest/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/NightlyTest.dir/build.make glog-build/CMakeFiles/NightlyTest.dir/build
.PHONY : NightlyTest/fast

#=============================================================================
# Target rules for targets named NightlyCoverage

# Build rule for target.
NightlyCoverage: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 NightlyCoverage
.PHONY : NightlyCoverage

# fast build rule for target.
NightlyCoverage/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/NightlyCoverage.dir/build.make glog-build/CMakeFiles/NightlyCoverage.dir/build
.PHONY : NightlyCoverage/fast

#=============================================================================
# Target rules for targets named NightlyMemCheck

# Build rule for target.
NightlyMemCheck: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 NightlyMemCheck
.PHONY : NightlyMemCheck

# fast build rule for target.
NightlyMemCheck/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/NightlyMemCheck.dir/build.make glog-build/CMakeFiles/NightlyMemCheck.dir/build
.PHONY : NightlyMemCheck/fast

#=============================================================================
# Target rules for targets named NightlySubmit

# Build rule for target.
NightlySubmit: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 NightlySubmit
.PHONY : NightlySubmit

# fast build rule for target.
NightlySubmit/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/NightlySubmit.dir/build.make glog-build/CMakeFiles/NightlySubmit.dir/build
.PHONY : NightlySubmit/fast

#=============================================================================
# Target rules for targets named ExperimentalStart

# Build rule for target.
ExperimentalStart: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ExperimentalStart
.PHONY : ExperimentalStart

# fast build rule for target.
ExperimentalStart/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ExperimentalStart.dir/build.make glog-build/CMakeFiles/ExperimentalStart.dir/build
.PHONY : ExperimentalStart/fast

#=============================================================================
# Target rules for targets named ExperimentalUpdate

# Build rule for target.
ExperimentalUpdate: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ExperimentalUpdate
.PHONY : ExperimentalUpdate

# fast build rule for target.
ExperimentalUpdate/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ExperimentalUpdate.dir/build.make glog-build/CMakeFiles/ExperimentalUpdate.dir/build
.PHONY : ExperimentalUpdate/fast

#=============================================================================
# Target rules for targets named ExperimentalConfigure

# Build rule for target.
ExperimentalConfigure: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ExperimentalConfigure
.PHONY : ExperimentalConfigure

# fast build rule for target.
ExperimentalConfigure/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ExperimentalConfigure.dir/build.make glog-build/CMakeFiles/ExperimentalConfigure.dir/build
.PHONY : ExperimentalConfigure/fast

#=============================================================================
# Target rules for targets named ExperimentalBuild

# Build rule for target.
ExperimentalBuild: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ExperimentalBuild
.PHONY : ExperimentalBuild

# fast build rule for target.
ExperimentalBuild/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ExperimentalBuild.dir/build.make glog-build/CMakeFiles/ExperimentalBuild.dir/build
.PHONY : ExperimentalBuild/fast

#=============================================================================
# Target rules for targets named ExperimentalTest

# Build rule for target.
ExperimentalTest: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ExperimentalTest
.PHONY : ExperimentalTest

# fast build rule for target.
ExperimentalTest/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ExperimentalTest.dir/build.make glog-build/CMakeFiles/ExperimentalTest.dir/build
.PHONY : ExperimentalTest/fast

#=============================================================================
# Target rules for targets named ExperimentalCoverage

# Build rule for target.
ExperimentalCoverage: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ExperimentalCoverage
.PHONY : ExperimentalCoverage

# fast build rule for target.
ExperimentalCoverage/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ExperimentalCoverage.dir/build.make glog-build/CMakeFiles/ExperimentalCoverage.dir/build
.PHONY : ExperimentalCoverage/fast

#=============================================================================
# Target rules for targets named ExperimentalMemCheck

# Build rule for target.
ExperimentalMemCheck: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ExperimentalMemCheck
.PHONY : ExperimentalMemCheck

# fast build rule for target.
ExperimentalMemCheck/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ExperimentalMemCheck.dir/build.make glog-build/CMakeFiles/ExperimentalMemCheck.dir/build
.PHONY : ExperimentalMemCheck/fast

#=============================================================================
# Target rules for targets named ExperimentalSubmit

# Build rule for target.
ExperimentalSubmit: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ExperimentalSubmit
.PHONY : ExperimentalSubmit

# fast build rule for target.
ExperimentalSubmit/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ExperimentalSubmit.dir/build.make glog-build/CMakeFiles/ExperimentalSubmit.dir/build
.PHONY : ExperimentalSubmit/fast

#=============================================================================
# Target rules for targets named ContinuousStart

# Build rule for target.
ContinuousStart: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ContinuousStart
.PHONY : ContinuousStart

# fast build rule for target.
ContinuousStart/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ContinuousStart.dir/build.make glog-build/CMakeFiles/ContinuousStart.dir/build
.PHONY : ContinuousStart/fast

#=============================================================================
# Target rules for targets named ContinuousUpdate

# Build rule for target.
ContinuousUpdate: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ContinuousUpdate
.PHONY : ContinuousUpdate

# fast build rule for target.
ContinuousUpdate/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ContinuousUpdate.dir/build.make glog-build/CMakeFiles/ContinuousUpdate.dir/build
.PHONY : ContinuousUpdate/fast

#=============================================================================
# Target rules for targets named ContinuousConfigure

# Build rule for target.
ContinuousConfigure: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ContinuousConfigure
.PHONY : ContinuousConfigure

# fast build rule for target.
ContinuousConfigure/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ContinuousConfigure.dir/build.make glog-build/CMakeFiles/ContinuousConfigure.dir/build
.PHONY : ContinuousConfigure/fast

#=============================================================================
# Target rules for targets named ContinuousBuild

# Build rule for target.
ContinuousBuild: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ContinuousBuild
.PHONY : ContinuousBuild

# fast build rule for target.
ContinuousBuild/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ContinuousBuild.dir/build.make glog-build/CMakeFiles/ContinuousBuild.dir/build
.PHONY : ContinuousBuild/fast

#=============================================================================
# Target rules for targets named ContinuousTest

# Build rule for target.
ContinuousTest: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ContinuousTest
.PHONY : ContinuousTest

# fast build rule for target.
ContinuousTest/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ContinuousTest.dir/build.make glog-build/CMakeFiles/ContinuousTest.dir/build
.PHONY : ContinuousTest/fast

#=============================================================================
# Target rules for targets named ContinuousCoverage

# Build rule for target.
ContinuousCoverage: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ContinuousCoverage
.PHONY : ContinuousCoverage

# fast build rule for target.
ContinuousCoverage/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ContinuousCoverage.dir/build.make glog-build/CMakeFiles/ContinuousCoverage.dir/build
.PHONY : ContinuousCoverage/fast

#=============================================================================
# Target rules for targets named ContinuousMemCheck

# Build rule for target.
ContinuousMemCheck: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ContinuousMemCheck
.PHONY : ContinuousMemCheck

# fast build rule for target.
ContinuousMemCheck/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ContinuousMemCheck.dir/build.make glog-build/CMakeFiles/ContinuousMemCheck.dir/build
.PHONY : ContinuousMemCheck/fast

#=============================================================================
# Target rules for targets named ContinuousSubmit

# Build rule for target.
ContinuousSubmit: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 ContinuousSubmit
.PHONY : ContinuousSubmit

# fast build rule for target.
ContinuousSubmit/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/ContinuousSubmit.dir/build.make glog-build/CMakeFiles/ContinuousSubmit.dir/build
.PHONY : ContinuousSubmit/fast

#=============================================================================
# Target rules for targets named glogbase

# Build rule for target.
glogbase: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 glogbase
.PHONY : glogbase

# fast build rule for target.
glogbase/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/glogbase.dir/build.make glog-build/CMakeFiles/glogbase.dir/build
.PHONY : glogbase/fast

#=============================================================================
# Target rules for targets named glog

# Build rule for target.
glog: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 glog
.PHONY : glog

# fast build rule for target.
glog/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/glog.dir/build.make glog-build/CMakeFiles/glog.dir/build
.PHONY : glog/fast

#=============================================================================
# Target rules for targets named glogtest

# Build rule for target.
glogtest: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 glogtest
.PHONY : glogtest

# fast build rule for target.
glogtest/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/glogtest.dir/build.make glog-build/CMakeFiles/glogtest.dir/build
.PHONY : glogtest/fast

#=============================================================================
# Target rules for targets named logging_unittest

# Build rule for target.
logging_unittest: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 logging_unittest
.PHONY : logging_unittest

# fast build rule for target.
logging_unittest/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/logging_unittest.dir/build.make glog-build/CMakeFiles/logging_unittest.dir/build
.PHONY : logging_unittest/fast

#=============================================================================
# Target rules for targets named stl_logging_unittest

# Build rule for target.
stl_logging_unittest: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 stl_logging_unittest
.PHONY : stl_logging_unittest

# fast build rule for target.
stl_logging_unittest/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/stl_logging_unittest.dir/build.make glog-build/CMakeFiles/stl_logging_unittest.dir/build
.PHONY : stl_logging_unittest/fast

#=============================================================================
# Target rules for targets named symbolize_unittest

# Build rule for target.
symbolize_unittest: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 symbolize_unittest
.PHONY : symbolize_unittest

# fast build rule for target.
symbolize_unittest/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/symbolize_unittest.dir/build.make glog-build/CMakeFiles/symbolize_unittest.dir/build
.PHONY : symbolize_unittest/fast

#=============================================================================
# Target rules for targets named demangle_unittest

# Build rule for target.
demangle_unittest: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 demangle_unittest
.PHONY : demangle_unittest

# fast build rule for target.
demangle_unittest/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/demangle_unittest.dir/build.make glog-build/CMakeFiles/demangle_unittest.dir/build
.PHONY : demangle_unittest/fast

#=============================================================================
# Target rules for targets named stacktrace_unittest

# Build rule for target.
stacktrace_unittest: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 stacktrace_unittest
.PHONY : stacktrace_unittest

# fast build rule for target.
stacktrace_unittest/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/stacktrace_unittest.dir/build.make glog-build/CMakeFiles/stacktrace_unittest.dir/build
.PHONY : stacktrace_unittest/fast

#=============================================================================
# Target rules for targets named utilities_unittest

# Build rule for target.
utilities_unittest: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 utilities_unittest
.PHONY : utilities_unittest

# fast build rule for target.
utilities_unittest/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/utilities_unittest.dir/build.make glog-build/CMakeFiles/utilities_unittest.dir/build
.PHONY : utilities_unittest/fast

#=============================================================================
# Target rules for targets named signalhandler_unittest

# Build rule for target.
signalhandler_unittest: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 signalhandler_unittest
.PHONY : signalhandler_unittest

# fast build rule for target.
signalhandler_unittest/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/signalhandler_unittest.dir/build.make glog-build/CMakeFiles/signalhandler_unittest.dir/build
.PHONY : signalhandler_unittest/fast

#=============================================================================
# Target rules for targets named cleanup_immediately_unittest

# Build rule for target.
cleanup_immediately_unittest: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 cleanup_immediately_unittest
.PHONY : cleanup_immediately_unittest

# fast build rule for target.
cleanup_immediately_unittest/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/cleanup_immediately_unittest.dir/build.make glog-build/CMakeFiles/cleanup_immediately_unittest.dir/build
.PHONY : cleanup_immediately_unittest/fast

#=============================================================================
# Target rules for targets named cleanup_with_absolute_prefix_unittest

# Build rule for target.
cleanup_with_absolute_prefix_unittest: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 cleanup_with_absolute_prefix_unittest
.PHONY : cleanup_with_absolute_prefix_unittest

# fast build rule for target.
cleanup_with_absolute_prefix_unittest/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/cleanup_with_absolute_prefix_unittest.dir/build.make glog-build/CMakeFiles/cleanup_with_absolute_prefix_unittest.dir/build
.PHONY : cleanup_with_absolute_prefix_unittest/fast

#=============================================================================
# Target rules for targets named cleanup_with_relative_prefix_unittest

# Build rule for target.
cleanup_with_relative_prefix_unittest: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 cleanup_with_relative_prefix_unittest
.PHONY : cleanup_with_relative_prefix_unittest

# fast build rule for target.
cleanup_with_relative_prefix_unittest/fast:
	$(MAKE) $(MAKESILENT) -f glog-build/CMakeFiles/cleanup_with_relative_prefix_unittest.dir/build.make glog-build/CMakeFiles/cleanup_with_relative_prefix_unittest.dir/build
.PHONY : cleanup_with_relative_prefix_unittest/fast

#=============================================================================
# Target rules for targets named minisql_shared

# Build rule for target.
minisql_shared: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 minisql_shared
.PHONY : minisql_shared

# fast build rule for target.
minisql_shared/fast:
	$(MAKE) $(MAKESILENT) -f bin/CMakeFiles/minisql_shared.dir/build.make bin/CMakeFiles/minisql_shared.dir/build
.PHONY : minisql_shared/fast

#=============================================================================
# Target rules for targets named main

# Build rule for target.
main: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 main
.PHONY : main

# fast build rule for target.
main/fast:
	$(MAKE) $(MAKESILENT) -f bin/CMakeFiles/main.dir/build.make bin/CMakeFiles/main.dir/build
.PHONY : main/fast

#=============================================================================
# Target rules for targets named minisql_test

# Build rule for target.
minisql_test: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 minisql_test
.PHONY : minisql_test

# fast build rule for target.
minisql_test/fast:
	$(MAKE) $(MAKESILENT) -f test/CMakeFiles/minisql_test.dir/build.make test/CMakeFiles/minisql_test.dir/build
.PHONY : minisql_test/fast

#=============================================================================
# Target rules for targets named minisql_test_main

# Build rule for target.
minisql_test_main: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 minisql_test_main
.PHONY : minisql_test_main

# fast build rule for target.
minisql_test_main/fast:
	$(MAKE) $(MAKESILENT) -f test/CMakeFiles/minisql_test_main.dir/build.make test/CMakeFiles/minisql_test_main.dir/build
.PHONY : minisql_test_main/fast

#=============================================================================
# Target rules for targets named buffer_pool_manager_test

# Build rule for target.
buffer_pool_manager_test: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 buffer_pool_manager_test
.PHONY : buffer_pool_manager_test

# fast build rule for target.
buffer_pool_manager_test/fast:
	$(MAKE) $(MAKESILENT) -f test/CMakeFiles/buffer_pool_manager_test.dir/build.make test/CMakeFiles/buffer_pool_manager_test.dir/build
.PHONY : buffer_pool_manager_test/fast

#=============================================================================
# Target rules for targets named clock_replacer_test

# Build rule for target.
clock_replacer_test: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 clock_replacer_test
.PHONY : clock_replacer_test

# fast build rule for target.
clock_replacer_test/fast:
	$(MAKE) $(MAKESILENT) -f test/CMakeFiles/clock_replacer_test.dir/build.make test/CMakeFiles/clock_replacer_test.dir/build
.PHONY : clock_replacer_test/fast

#=============================================================================
# Target rules for targets named lru_replacer_test

# Build rule for target.
lru_replacer_test: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 lru_replacer_test
.PHONY : lru_replacer_test

# fast build rule for target.
lru_replacer_test/fast:
	$(MAKE) $(MAKESILENT) -f test/CMakeFiles/lru_replacer_test.dir/build.make test/CMakeFiles/lru_replacer_test.dir/build
.PHONY : lru_replacer_test/fast

#=============================================================================
# Target rules for targets named catalog_test

# Build rule for target.
catalog_test: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 catalog_test
.PHONY : catalog_test

# fast build rule for target.
catalog_test/fast:
	$(MAKE) $(MAKESILENT) -f test/CMakeFiles/catalog_test.dir/build.make test/CMakeFiles/catalog_test.dir/build
.PHONY : catalog_test/fast

#=============================================================================
# Target rules for targets named heap_test

# Build rule for target.
heap_test: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 heap_test
.PHONY : heap_test

# fast build rule for target.
heap_test/fast:
	$(MAKE) $(MAKESILENT) -f test/CMakeFiles/heap_test.dir/build.make test/CMakeFiles/heap_test.dir/build
.PHONY : heap_test/fast

#=============================================================================
# Target rules for targets named b_plus_tree_index_test

# Build rule for target.
b_plus_tree_index_test: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 b_plus_tree_index_test
.PHONY : b_plus_tree_index_test

# fast build rule for target.
b_plus_tree_index_test/fast:
	$(MAKE) $(MAKESILENT) -f test/CMakeFiles/b_plus_tree_index_test.dir/build.make test/CMakeFiles/b_plus_tree_index_test.dir/build
.PHONY : b_plus_tree_index_test/fast

#=============================================================================
# Target rules for targets named b_plus_tree_test

# Build rule for target.
b_plus_tree_test: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 b_plus_tree_test
.PHONY : b_plus_tree_test

# fast build rule for target.
b_plus_tree_test/fast:
	$(MAKE) $(MAKESILENT) -f test/CMakeFiles/b_plus_tree_test.dir/build.make test/CMakeFiles/b_plus_tree_test.dir/build
.PHONY : b_plus_tree_test/fast

#=============================================================================
# Target rules for targets named index_iterator_test

# Build rule for target.
index_iterator_test: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 index_iterator_test
.PHONY : index_iterator_test

# fast build rule for target.
index_iterator_test/fast:
	$(MAKE) $(MAKESILENT) -f test/CMakeFiles/index_iterator_test.dir/build.make test/CMakeFiles/index_iterator_test.dir/build
.PHONY : index_iterator_test/fast

#=============================================================================
# Target rules for targets named index_roots_page_test

# Build rule for target.
index_roots_page_test: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 index_roots_page_test
.PHONY : index_roots_page_test

# fast build rule for target.
index_roots_page_test/fast:
	$(MAKE) $(MAKESILENT) -f test/CMakeFiles/index_roots_page_test.dir/build.make test/CMakeFiles/index_roots_page_test.dir/build
.PHONY : index_roots_page_test/fast

#=============================================================================
# Target rules for targets named tuple_test

# Build rule for target.
tuple_test: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 tuple_test
.PHONY : tuple_test

# fast build rule for target.
tuple_test/fast:
	$(MAKE) $(MAKESILENT) -f test/CMakeFiles/tuple_test.dir/build.make test/CMakeFiles/tuple_test.dir/build
.PHONY : tuple_test/fast

#=============================================================================
# Target rules for targets named disk_manager_test

# Build rule for target.
disk_manager_test: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 disk_manager_test
.PHONY : disk_manager_test

# fast build rule for target.
disk_manager_test/fast:
	$(MAKE) $(MAKESILENT) -f test/CMakeFiles/disk_manager_test.dir/build.make test/CMakeFiles/disk_manager_test.dir/build
.PHONY : disk_manager_test/fast

#=============================================================================
# Target rules for targets named table_heap_test

# Build rule for target.
table_heap_test: cmake_check_build_system
	$(MAKE) $(MAKESILENT) -f CMakeFiles/Makefile2 table_heap_test
.PHONY : table_heap_test

# fast build rule for target.
table_heap_test/fast:
	$(MAKE) $(MAKESILENT) -f test/CMakeFiles/table_heap_test.dir/build.make test/CMakeFiles/table_heap_test.dir/build
.PHONY : table_heap_test/fast

# Help Target
help:
	@echo "The following are some of the valid targets for this Makefile:"
	@echo "... all (the default if no target is provided)"
	@echo "... clean"
	@echo "... depend"
	@echo "... edit_cache"
	@echo "... install"
	@echo "... install/local"
	@echo "... install/strip"
	@echo "... list_install_components"
	@echo "... package"
	@echo "... package_source"
	@echo "... rebuild_cache"
	@echo "... test"
	@echo "... Continuous"
	@echo "... ContinuousBuild"
	@echo "... ContinuousConfigure"
	@echo "... ContinuousCoverage"
	@echo "... ContinuousMemCheck"
	@echo "... ContinuousStart"
	@echo "... ContinuousSubmit"
	@echo "... ContinuousTest"
	@echo "... ContinuousUpdate"
	@echo "... Experimental"
	@echo "... ExperimentalBuild"
	@echo "... ExperimentalConfigure"
	@echo "... ExperimentalCoverage"
	@echo "... ExperimentalMemCheck"
	@echo "... ExperimentalStart"
	@echo "... ExperimentalSubmit"
	@echo "... ExperimentalTest"
	@echo "... ExperimentalUpdate"
	@echo "... Nightly"
	@echo "... NightlyBuild"
	@echo "... NightlyConfigure"
	@echo "... NightlyCoverage"
	@echo "... NightlyMemCheck"
	@echo "... NightlyMemoryCheck"
	@echo "... NightlyStart"
	@echo "... NightlySubmit"
	@echo "... NightlyTest"
	@echo "... NightlyUpdate"
	@echo "... b_plus_tree_index_test"
	@echo "... b_plus_tree_test"
	@echo "... buffer_pool_manager_test"
	@echo "... catalog_test"
	@echo "... cleanup_immediately_unittest"
	@echo "... cleanup_with_absolute_prefix_unittest"
	@echo "... cleanup_with_relative_prefix_unittest"
	@echo "... clock_replacer_test"
	@echo "... demangle_unittest"
	@echo "... disk_manager_test"
	@echo "... glog"
	@echo "... glogbase"
	@echo "... glogtest"
	@echo "... gtest"
	@echo "... gtest_main"
	@echo "... heap_test"
	@echo "... index_iterator_test"
	@echo "... index_roots_page_test"
	@echo "... logging_unittest"
	@echo "... lru_replacer_test"
	@echo "... main"
	@echo "... minisql_shared"
	@echo "... minisql_test"
	@echo "... minisql_test_main"
	@echo "... signalhandler_unittest"
	@echo "... stacktrace_unittest"
	@echo "... stl_logging_unittest"
	@echo "... symbolize_unittest"
	@echo "... table_heap_test"
	@echo "... tuple_test"
	@echo "... utilities_unittest"
.PHONY : help



#=============================================================================
# Special targets to cleanup operation of make.

# Special rule to run CMake to check the build system integrity.
# No rule that depends on this can have commands that come from listfiles
# because they might be regenerated.
cmake_check_build_system:
	$(CMAKE_COMMAND) -S$(CMAKE_SOURCE_DIR) -B$(CMAKE_BINARY_DIR) --check-build-system CMakeFiles/Makefile.cmake 0
.PHONY : cmake_check_build_system

