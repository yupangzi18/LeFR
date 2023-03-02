# LeFR

# Dependencies

Before the following steps,build [brpc](https://github.com/apache/incubator-brpc) and [braft](https://github.com/baidu/braft) which is the main dependency of LeFR.

```sh
sudo apt-get update
sudo apt-get install -y zip make cmake g++ libjemalloc-dev libboost-dev libgoogle-glog-dev
```

# Build

```
cd /LeFR
./compile.sh
```

# Run

```
./bench_ycsb  --id=0 --servers="node0;node1;node2;..." --protocol="LeFR/GPAC/Tapir" --thread=16 --partition_num=80

./bench_tpcc  --id=0 --servers="node0;node1;node2;..." --protocol="LeFR/GPAC/Tapir" --thread=16 --partition_num=80
```
