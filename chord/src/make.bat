thrift-0.9.3.exe --gen java Address.thrift
thrift-0.9.3.exe --gen java NodeService.thrift
thrift-0.9.3.exe --gen java SuperNodeService.thrift
del Address.java
del NodeService.java
del SuperNodeService.java
copy gen-java\Address.java .\
copy gen-java\NodeService.java .\
copy gen-java\SuperNodeService.java .\
rd gen-java /s /q

