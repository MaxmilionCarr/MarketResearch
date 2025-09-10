from pathlib import Path
from setuptools import setup, Extension, find_packages
import pybind11, platform, os

# Always run from root

ROOT = Path(__file__).parent.resolve()
# point to libraries directory
CPP_DIR = ROOT / "libraries"

# collect all .cpp files recursively
cpp_files = sorted(CPP_DIR.rglob("*.cpp"))
if not cpp_files:
    raise SystemExit("No C++ source files found in ./libraries/**/*.cpp")
               # your .cpp files live here

# compiler flags
system = platform.system()
if system == "Windows":
    cxx_flags = ["/O2", "/std:c++17", "/EHsc"]
    link_args = []
    if os.environ.get("NO_OMP") != "1":
        cxx_flags += ["/openmp"]
else:
    cxx_flags = ["-O3", "-std=c++17", "-Wall", "-fPIC"]
    link_args = []
    if os.environ.get("NO_OMP") != "1":
        cxx_flags += ["-fopenmp"]
        link_args += ["-fopenmp"]

# one extension per file; module name = filename stem (top-level import)
ext_modules = [
    Extension(
        cpp.stem,
        sources=[str(cpp.relative_to(ROOT))],
        include_dirs=[pybind11.get_include()],
        language="c++",
        extra_compile_args=cxx_flags,
        extra_link_args=link_args,
    )
    for cpp in cpp_files
]

setup(
    name="optionpricer",
    version="0.1.0",
    description="C++ pricers via pybind11",
    ext_modules=ext_modules,
    packages=find_packages(),        # <-- disable package discovery
    py_modules=[],      # <-- (explicitly none)
)
