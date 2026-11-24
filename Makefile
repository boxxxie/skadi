SRC         := skadi
TESTS       := tests
SPECS       := specs
BIN         := bin
CHECK_FLAGS := -vet -strict-style -disallow-do -warnings-as-errors
ACTOR_DIR   := $(if $(wildcard ../sleipnir/actor),../sleipnir/actor,deps/sleipnir/actor)
ACTOR_COL   := -collection:actor=$(ACTOR_DIR)
HUGI_COL    := $(if $(wildcard ../hugi/hugi),-collection:hugi=../hugi/hugi,$(if $(wildcard .tmp/hugi/hugi),-collection:hugi=.tmp/hugi/hugi -collection:obench=.tmp/hugi/deps/obench/obench,))

# Apache Arrow C++ paths — prefer system pkg-config, fall back to bundled PyArrow.
HAS_PKG_ARROW := $(shell pkg-config --exists arrow arrow-compute 2>/dev/null && echo 1 || echo 0)
ifeq ($(HAS_PKG_ARROW),1)
ARROW_IDIR       := $(shell pkg-config --variable=includedir arrow 2>/dev/null)
ARROW_LDIR       := $(shell pkg-config --variable=libdir arrow 2>/dev/null)
ARROW_CPP_FLAGS  := -std=c++20 -O3 $(shell pkg-config --cflags arrow arrow-compute)
ARROW_LINK_FLAGS := $(shell pkg-config --libs arrow arrow-compute) -Wl,-rpath,$(ARROW_LDIR)
PARQUET_LINK_FLAGS := $(shell pkg-config --libs arrow parquet) -Wl,-rpath,$(ARROW_LDIR)
else
ARROW_IDIR := $(shell python3 -c "import pyarrow; print(pyarrow.get_include())" 2>/dev/null)
ARROW_LDIR := $(shell python3 -c "import pyarrow; print(pyarrow.get_library_dirs()[0])" 2>/dev/null)
ARROW_VER  ?= $(shell python3 -c "import pyarrow, glob, os; d = pyarrow.get_library_dirs()[0]; m = sorted(glob.glob(os.path.join(d, 'libarrow.so.*'))); print(os.path.basename(m[0]).split('libarrow.so.')[1] if m else '2500')" 2>/dev/null)
ARROW_COMPUTE_LIB := $(shell python3 -c "import pyarrow, glob, os; d = pyarrow.get_library_dirs()[0]; m = sorted(glob.glob(os.path.join(d, 'libarrow_compute.so.*'))); print('-l:' + os.path.basename(m[0]) if m else '')" 2>/dev/null)

ARROW_CPP_FLAGS  := -std=c++20 -O3 -I$(ARROW_IDIR)
ARROW_LINK_FLAGS := -L$(ARROW_LDIR) \
    $(ARROW_COMPUTE_LIB) \
    -l:libarrow.so.$(ARROW_VER) \
    -Wl,-rpath,$(ARROW_LDIR)

PARQUET_LINK_FLAGS := -L$(ARROW_LDIR) \
    -l:libparquet.so.$(ARROW_VER) \
    -l:libarrow.so.$(ARROW_VER) \
    -Wl,-rpath,$(ARROW_LDIR)
endif

TEST_DATA ?= test_data.parquet

.PHONY: all check test test-specs test-cpp bench-odin bench-python bench bench-compare \
        build-arrow-capi bench-arrow-cpp \
        build-parquet-capi parquet-odin parquet-ffi bench-parquet \
        csv-to-parquet-odin csv-to-parquet-ffi clean

all: test

$(BIN):
	mkdir -p $(BIN)

lib:
	mkdir -p lib

# ── Static analysis & Odin tests (no C++ dependency) ─────────────────────────

check:
	odin check $(SRC) -no-entry-point $(CHECK_FLAGS)
	odin check $(SRC)/pipeline -no-entry-point $(CHECK_FLAGS) $(ACTOR_COL)
	odin check $(ACTOR_DIR) -no-entry-point $(CHECK_FLAGS)
	odin check $(TESTS) -no-entry-point $(CHECK_FLAGS) $(ACTOR_COL)
	odin check $(SPECS) -no-entry-point $(CHECK_FLAGS) $(ACTOR_COL)
	odin check tests_cpp -no-entry-point $(CHECK_FLAGS)
	odin check examples/quickstart $(CHECK_FLAGS)
	odin check examples/csv_to_parquet $(CHECK_FLAGS)
	odin check examples/parquet_to_csv $(CHECK_FLAGS)
	odin check benchmarks/csv_to_parquet_ffi $(CHECK_FLAGS)
	odin check benchmarks/parquet_to_csv_ffi $(CHECK_FLAGS)
	odin check benchmarks/odin $(CHECK_FLAGS) $(ACTOR_COL) $(HUGI_COL)
	odin check benchmarks/odin_c $(CHECK_FLAGS)

test-specs: $(BIN)
	odin test $(SPECS) -out:$(BIN)/spec_runner $(CHECK_FLAGS) $(ACTOR_COL)
	python3 -m unittest benchmarks/test_compare_1v1.py

test: $(BIN)
	odin test $(TESTS) -out:$(BIN)/test_runner $(CHECK_FLAGS) $(ACTOR_COL)
	odin test $(SPECS) -out:$(BIN)/spec_runner $(CHECK_FLAGS) $(ACTOR_COL)
	python3 -m unittest benchmarks/test_compare_1v1.py

# ── Arrow C++ shared library (for Odin FFI) ───────────────────────────────────

lib/libarrow_capi.so: benchmarks/cpp/arrow_capi.cpp benchmarks/cpp/arrow_capi.h | lib
	g++ $(ARROW_CPP_FLAGS) -shared -fPIC \
	    benchmarks/cpp/arrow_capi.cpp \
	    $(ARROW_LINK_FLAGS) \
	    -Wl,-rpath,'$$ORIGIN' \
	    -o $@

build-arrow-capi: lib/libarrow_capi.so

# ── Odin tests that link Arrow C++ ───────────────────────────────────────────

test-cpp: lib/libarrow_capi.so $(BIN)
	odin test tests_cpp -out:$(BIN)/test_runner_cpp $(CHECK_FLAGS) \
	    -extra-linker-flags="-Wl,-rpath,$(realpath lib) -Wl,-rpath,$(ARROW_LDIR)"

# ── Standalone Arrow C++ benchmark binary ─────────────────────────────────────

bin/bench_arrow_cpp: benchmarks/cpp/bench_arrow_main.cpp \
                     benchmarks/cpp/arrow_capi.cpp \
                     benchmarks/cpp/arrow_capi.h | $(BIN)
	g++ $(ARROW_CPP_FLAGS) \
	    benchmarks/cpp/bench_arrow_main.cpp \
	    benchmarks/cpp/arrow_capi.cpp \
	    $(ARROW_LINK_FLAGS) \
	    -o $@

bench-arrow-cpp: bin/bench_arrow_cpp
	bin/bench_arrow_cpp

# ── Python benchmarks ─────────────────────────────────────────────────────────

bench-odin: $(BIN)
	odin run benchmarks/odin -out:$(BIN)/bench_runner -o:speed -microarch:native $(ACTOR_COL) $(HUGI_COL)

bench-python:
	python3 benchmarks/python/bench_array.py
	python3 benchmarks/python/bench_compute.py

bench: bench-odin bench-python

# ── Full 3-way comparison ─────────────────────────────────────────────────────

bench-compare: bin/bench_arrow_cpp $(BIN)
	bash benchmarks/compare.sh

# ── Parquet → CSV: Arrow C++ FFI shared library ───────────────────────────────

lib/libparquet_capi.so: benchmarks/cpp/parquet_capi.cpp benchmarks/cpp/parquet_capi.h | lib
	g++ $(ARROW_CPP_FLAGS) -shared -fPIC \
	    benchmarks/cpp/parquet_capi.cpp \
	    $(PARQUET_LINK_FLAGS) \
	    -Wl,-rpath,'$$ORIGIN' \
	    -o $@

build-parquet-capi: lib/libparquet_capi.so

# ── Parquet → CSV programs ────────────────────────────────────────────────────

bin/parquet_to_csv_odin: examples/parquet_to_csv/*.odin | $(BIN)
	odin build examples/parquet_to_csv -out:$@ -o:speed

bin/parquet_to_csv_ffi: benchmarks/parquet_to_csv_ffi/main.odin lib/libparquet_capi.so | $(BIN)
	odin build benchmarks/parquet_to_csv_ffi -out:$@ -o:speed \
	    -extra-linker-flags="-Wl,-rpath,$(realpath lib) -Wl,-rpath,$(ARROW_LDIR)"

parquet-odin: bin/parquet_to_csv_odin

parquet-ffi: bin/parquet_to_csv_ffi

# ── CSV → Parquet programs ────────────────────────────────────────────────────

bin/csv_to_parquet_odin: examples/csv_to_parquet/*.odin | $(BIN)
	odin build examples/csv_to_parquet -out:$@ -o:speed

bin/csv_to_parquet_ffi: benchmarks/csv_to_parquet_ffi/main.odin lib/libparquet_capi.so | $(BIN)
	odin build benchmarks/csv_to_parquet_ffi -out:$@ -o:speed \
	    -extra-linker-flags="-Wl,-rpath,$(realpath lib) -Wl,-rpath,$(ARROW_LDIR)"

csv-to-parquet-odin: bin/csv_to_parquet_odin

csv-to-parquet-ffi: bin/csv_to_parquet_ffi

# ── Parquet benchmark ─────────────────────────────────────────────────────────

bench-parquet: bin/parquet_to_csv_odin bin/parquet_to_csv_ffi
	bash benchmarks/parquet_bench.sh $(TEST_DATA)

.PHONY: bench-1v1
bench-1v1: bin/bench_arrow_cpp
	python3 benchmarks/compare_1v1.py

clean:
	rm -rf $(BIN) lib
