thrift-0.9.3.exe --gen java Address.thrift
thrift-0.9.3.exe --gen java NodeService.thrift
thrift-0.9.3.exe --gen java CoordinatorService.thrift

del Address.java
del NodeService.java
del CoordinatorService.java

copy gen-java\Address.java .\
copy gen-java\NodeService.java .\
copy gen-java\CoordinatorService.java .\

rd gen-java /s /q

