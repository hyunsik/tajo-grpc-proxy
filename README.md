# grpc for Apache Tajo

This is GRpc proxy server for Apache Tajo. Because hadoop depends on protobuf 2.5.0, it is hard for Tajo to 
have higher protobuf version (>= 2.6.1), required for grpc. This proxy server allows users to directly use grpc 
to access Tajo clusters.

