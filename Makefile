BUILD_DIR = WithMakeBuild

DEBUG = 1
CXX_STANDARD = c++17
CXX = g++
CXXFLAGS = -fPIC
CPPFLAGS =

COMPILER_CALL = $(CXX)

ifeq ($(DEBUG), 1)
        CXXFLAGS += -g -O0
else
        CXXFLAGS += -O3
endif

LDFLAGS = -shared -Wl,-soname=$(ADBC_DRIVER_NETEZZA_LIB)

NETEZZA_DRIVER_SOURCE_DIR = src/c
NETEZZA_DRIVER_INCLUDE_DIR = src/c/nz_include
NETEZZA_DRIVER_LIB_DIR = src/c/nz_lib
NETEZZA_DRIVER_LIB = libnzpq.so
NETEZZA_DRIVER_SOURCE_FILE=\
	connection.cc \
	database.cc \
	error.cc \
	netezza.cc \
	netezza_copy_reader.cc \
	result_helper.cc \
	statement.cc

ADBC_DRIVER_NETEZZA_LIB = libadbc_driver_netezza.so

ARROW_ADBC_DIR = arrow-adbc
ARROW_ADBC_BUILD_DIR = $(ARROW_ADBC_DIR)/build

NETEZZA_SOURCE_FILES = $(addprefix $(NETEZZA_DRIVER_SOURCE_DIR)/, $(NETEZZA_DRIVER_SOURCE_FILE))
OBJS = $(patsubst $(NETEZZA_DRIVER_SOURCE_DIR)/%.cc, $(BUILD_DIR)/%.o, $(NETEZZA_SOURCE_FILES))

ARRAOW_ADBC_DIR = arrow-adbc
ARRAOW_ADBC_DRIVER_DIR = arrow-adbc/c/driver
ARRAOW_ADBC_VENDOR_DIR = arrow-adbc/c/vendor

NZPYADBC_DIR = src/python

C_TAR_SOURCE_DIR = adbc_driver_netezza
C_TAR_LIB64_DIR = $(C_TAR_SOURCE_DIR)/lib64
C_TAR_SOURCE_INCLUDE_DIR = $(C_TAR_SOURCE_DIR)/include
C_TARBALL_NAME = adbc_driver_netezza.tgz

CPPFLAGS = -I $(ARRAOW_ADBC_DIR) -I $(ARRAOW_ADBC_DRIVER_DIR) -I $(ARRAOW_ADBC_VENDOR_DIR) \
-I $(NETEZZA_DRIVER_SOURCE_DIR) -I $(NETEZZA_DRIVER_INCLUDE_DIR)

all: create_build_dir copy_netezza_driver_lib create_test_dir update_symbols_map_file update_cmakelists_txt_file \
run_cmake_adbc build_netezza build_nzpyadbc make_c_tarball remove_text_added

compile_without_tests: create_build_dir copy_netezza_driver_lib run_cmake_adbc_without_tests build_netezza \
build_nzpyadbc make_c_tarball

update_symbols_map_file:
		sed -i '/SqliteDriverInit;/a \
			NetezzaDriverInit;' $(ARRAOW_ADBC_DIR)/c/symbols.map

update_cmakelists_txt_file:
		sed -i '/config_summary_message()/a \
if(ADBC_DRIVER_NETEZZA) \
  add_subdirectory(driver/netezza) \
endif()' $(ARRAOW_ADBC_DIR)/c/CMakeLists.txt

create_test_dir:
	mkdir -p $(ARRAOW_ADBC_DRIVER_DIR)/netezza
	cp -r $(NETEZZA_DRIVER_SOURCE_DIR)/* $(ARRAOW_ADBC_DRIVER_DIR)/netezza/

run_cmake_adbc: create_build_dir
	@echo "Running cmake adbc"
	@[ -d $(ARROW_ADBC_BUILD_DIR) ] || mkdir -p $(ARROW_ADBC_BUILD_DIR)
	cd $(ARROW_ADBC_BUILD_DIR) && cmake ../c -DCMAKE_BUILD_TYPE=Debug -DADBC_BUILD_TESTS=ON -DADBC_DRIVER_NETEZZA=ON && \
	make -j && cp vendor/nanoarrow/libnanoarrow.a ../../$(BUILD_DIR)/libnanoarrow.a && \
	 cp driver/common/libadbc_driver_common.a ../../$(BUILD_DIR)/libadbc_driver_common.a

run_cmake_adbc_without_tests: create_build_dir
	@echo "Running cmake adbc"
	@[ -d $(ARROW_ADBC_BUILD_DIR) ] || mkdir -p $(ARROW_ADBC_BUILD_DIR)
	cd $(ARROW_ADBC_BUILD_DIR) && cmake ../c -DCMAKE_BUILD_TYPE=Debug && make -j && \
	 cp vendor/nanoarrow/libnanoarrow.a ../../$(BUILD_DIR)/libnanoarrow.a && \
	 cp driver/common/libadbc_driver_common.a ../../$(BUILD_DIR)/libadbc_driver_common.a

build_netezza: create_build_dir copy_netezza_driver_lib run_cmake_adbc $(OBJS)
	@echo "NETEZZA_SOURCE_FILES = $(NETEZZA_SOURCE_FILES)"
	@echo "OBJS = $(OBJS)"
	@echo "CPP FLAGS = $(CPPFLAGS)"
	$(COMPILER_CALL) $(CXXFLAGS) $(LDFLAGS) -o $(BUILD_DIR)/$(ADBC_DRIVER_NETEZZA_LIB) $(OBJS) -lkrb5 -lssl \
	 $(BUILD_DIR)/libnanoarrow.a $(BUILD_DIR)/libadbc_driver_common.a -L$(BUILD_DIR)/ -Wl,-rpath='$$ORIGIN' -lnzpq

build_nzpyadbc: build_netezza
	@cp $(BUILD_DIR)/$(ADBC_DRIVER_NETEZZA_LIB) $(NZPYADBC_DIR)/adbc_driver_netezza/
	@cp $(NETEZZA_DRIVER_LIB_DIR)/$(NETEZZA_DRIVER_LIB) $(NZPYADBC_DIR)/adbc_driver_netezza/

	cd $(NZPYADBC_DIR) && export ADBC_NETEZZA_LIBRARY=$(CURDIR)/$(BUILD_DIR)/$(ADBC_DRIVER_NETEZZA_LIB) && \
	python3 -m pip install --upgrade build --user && python3 -m build

make_c_tarball: build_netezza
	+@[ -d $(BUILD_DIR)/$(C_TAR_SOURCE_DIR) ] || mkdir -p $(BUILD_DIR)/$(C_TAR_SOURCE_DIR)
	+@[ -d $(BUILD_DIR)/$(C_TAR_SOURCE_INCLUDE_DIR) ] || mkdir -p $(BUILD_DIR)/$(C_TAR_SOURCE_INCLUDE_DIR)
	+@[ -d $(BUILD_DIR)/$(C_TAR_LIB64_DIR)] || mkdir -p $(BUILD_DIR)/$(C_TAR_LIB64_DIR)
	@cp -r $(NETEZZA_DRIVER_INCLUDE_DIR)/* $(BUILD_DIR)/$(C_TAR_SOURCE_INCLUDE_DIR)/
	@cp $(BUILD_DIR)/$(ADBC_DRIVER_NETEZZA_LIB) $(BUILD_DIR)/$(C_TAR_LIB64_DIR)/$(ADBC_DRIVER_NETEZZA_LIB)
	@cp $(NETEZZA_DRIVER_LIB_DIR)/$(NETEZZA_DRIVER_LIB) $(BUILD_DIR)/$(C_TAR_LIB64_DIR)/$(NETEZZA_DRIVER_LIB)
	@cp $(NETEZZA_DRIVER_SOURCE_DIR)/*.h $(BUILD_DIR)/$(C_TAR_SOURCE_INCLUDE_DIR)/
	cd $(BUILD_DIR)/$(C_TAR_SOURCE_DIR)/ && tar -zcf ../$(C_TARBALL_NAME) .

$(BUILD_DIR)/%.o: $(NETEZZA_DRIVER_SOURCE_DIR)/%.cc
	@echo "Building files."
	$(COMPILER_CALL) $(CXXFLAGS) -std=$(CXX_STANDARD) $(CPPFLAGS) -c $< -o $@

copy_netezza_driver_lib:
	@echo "Copying netezza driver lib."
	@cp $(NETEZZA_DRIVER_LIB_DIR)/$(NETEZZA_DRIVER_LIB) $(BUILD_DIR)/$(NETEZZA_DRIVER_LIB)

create_build_dir:
	@echo "In create_build_dir rule."
	+@[ -d $(BUILD_DIR) ] || mkdir -p $(BUILD_DIR)

remove_text_added:
	sed -i 's/NetezzaDriverInit;//' $(ARRAOW_ADBC_DIR)/c/symbols.map
	head -n -3 $(ARRAOW_ADBC_DIR)/c/CMakeLists.txt > temp && mv temp $(ARRAOW_ADBC_DIR)/c/CMakeLists.txt

clean:
	@rm -rf $(COMPILER_OBJECT) $(ADBC_DRIVER_NETEZZA_LIB)
	@rm -rf $(ARROW_ADBC_BUILD_DIR)
	@rm -rf $(BUILD_DIR)
	@rm -rf $(NZPYADBC_DIR)/dist $(NZPYADBC_DIR)/adbc_driver_netezza.egg-info $(NZPYADBC_DIR)/adbc_driver_netezza/__pycache__
	@rm -rf $(NZPYADBC_DIR)/adbc_driver_netezza/$(ADBC_DRIVER_NETEZZA_LIB) $(NZPYADBC_DIR)/adbc_driver_netezza/$(NETEZZA_DRIVER_LIB)
	@rm -rf $(ARRAOW_ADBC_DRIVER_DIR)/netezza
