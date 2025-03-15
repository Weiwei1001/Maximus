```bash
export PATH=/usr/local/cuda-12.5/bin/:$PATH
export LD_LIBRARY_PATH=/usr/local/cuda-12.5/lib64:$LD_LIBRARY_PATH

export CUDF_HOME=$(pwd)/cudf

git clone https://github.com/rapidsai/cudf.git $CUDF_HOME

# # checkout this commit:
https://github.com/rapidsai/cudf/commit/8068a2d616b6647bcd80720a2c24af858cbffd2d

cd $CUDF_HOME

export INSTALL_PREFIX=$HOME/cudf_install

CC=nvc CXX=nvc++ INSTALL_PREFIX=$HOME/cudf_install ./build.sh libcudf --disable_nvtx --ptds --cmake-args=\"-DCMAKE_PREFIX_PATH=$HOME/arrow_install\"

export spdlog_DIR=$HOME/cudf_install/lib64/cmake
export fmt_DIR=$HOME/cudf_install/lib64/cmake
export rmm_DIR=$HOME/cudf_install/lib64/cmake
export nvcomp_DIR=$HOME/cudf_install/lib/cmake
export Parquet_DIR=$HOME/libs/lib64/cmake
export ArrowAcero_DIR=$HOME/libs/lib64/cmake
export Arrow_DIR=$HOME/libs/lib64/cmake

export CMAKE_PREFIX_PATH=$HOME/arrow_install:$HOME/cudf_install:$HOME/taskflow_install:$HOME/caliper_install

cmake -DMAXIMUS_WITH_TESTS=ON -DCMAKE_BUILD_TYPE=Release -DMAXIMUS_WITH_GPU=ON -DCMAKE_PREFIX_PATH=$CMAKE_PREFIX_PATH ..

# compile
make -j 8
```
