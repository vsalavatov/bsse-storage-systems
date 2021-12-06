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
	[-verbose] 
	[-port <port> (default=4242)] 
	[-logs <path> (default=data/)] 
	[-max-conns <number> (default=32)] 
	[-scatter-bits <number> (default=6) -- number of parallel hash tables is 2^scatter bits]
```
