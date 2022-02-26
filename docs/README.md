---
description: Here you can learn how to start working with Reduct Storage
---

# 💡 Getting Started

Current version support only Linux OS and was tested on AMD64 platform

### Start With Docker

The easiest way to start using Reduct Storage is to run Docker image:

```
docker run -p 8383:8383 -v ${PWD}/data:/data ghcr.io/reduct-storage/reduct-storage:main 
```

The storage will be available on port http://127.0.01:8383 and store data in `./data` directory. You may check if it's working by a simple HTTP request:

```
curl http://127.0.0.1:8383/info
```

### Build Manually

To build the storage from source code, you need:

* GCC 11.2 or higher
* CMake 18 or higher
* conan

On Ubuntu 21.10 you can install by commands:

```
sudo apt install build-essential cmake python3-pip
sudo pip3 install conan
```

After having all the requirements installed, you can build the storage:

```
mkdir build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j
```

Finally, you can launch the storage:

```
RS_DATA_PATH=./data bin/reduct-storage
```

## Environment Variables

The storage can be customized by the following environment variables:

| Name                | Default   | Description                                                |
| ------------------- | --------- | ---------------------------------------------------------- |
| RS\_LOG\_LEVEL      | INFO      | Logging level, can be: TRACE, DEBUG, INFO, WARNING, ERROR  |
| RS\_HOST            | 0.0.0.0.0 | Listening IP address                                       |
| RS\_PORT            | 8383      | Listening port                                             |
| RS\_API\_BASE\_PATH | /         | Prefix for all URLs of requests                            |
| RS\_DATA\_PATH      | /data     | Path to a folder where the storage must store all the data |
|                     |           |                                                            |

