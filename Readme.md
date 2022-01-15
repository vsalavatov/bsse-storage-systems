# Persistent Hash Table

### Build instructions
```shell
$ make protoc-plugin
$ make protoc
$ make 
```

### Run instructions
```shell
$ ./server
  -ht-scatter-bits int
    	amount of hash bits used to scatter keys between hashtables (default 3)
  -kvs-scatter-bits int
    	amount of hash bits used to scatter keys between data storages (default 5)
  -logs string
    	path to the folder where data is located (default "data")
  -max-conns int
    	maximum concurrent connections to handle (default 32)
  -port int
    	the port server will listen on (default 4242)
  -verbose
    	print logs
```
