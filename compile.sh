#!/bin/bash

rm -rf CMakeFiles/ CMakeCache.txt
rm -rf data 
cmake -DCMAKE_BUILD_TYPE=Debug
make -j 8 
