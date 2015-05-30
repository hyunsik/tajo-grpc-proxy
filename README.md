# grpc for Apache Tajo

This is Grpc proxy server for Apache Tajo. Because hadoop depends on protobuf 2.5.0, it is hard for Tajo to 
use higher protobuf version (>= 2.6.1), required for grpc which provides various language bindings. This proxy server allows users to access a Tajo cluster through grpc.

# Author
 * Hyunsik Choi (hyunsik dot choi at gmail dot com)

# License
 * [Apache License 2.0] (http://www.apache.org/licenses/LICENSE-2.0)

# Building

*Prerequisites*
 * maven 3.0 or higher
 * JDK 7 or higher

```
git clone https://github.com/hyunsik/tajo-grpc-proxy.git
./build.sh
```

# Launching
```
./start-proxy.sh 
usage: ./start-proxy.sh [server address] [listen address]

./start-proxy.sh localhost:26002 localhost:28002
```

# See Also
 * https://github.com/grpc/grpc-java
