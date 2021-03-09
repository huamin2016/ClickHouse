export CC=gcc-9
export CXX=g++-9
cd build
cmake -DENABLE_TESTS=OFF -DWERROR=OFF ../
