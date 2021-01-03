include "Address.thrift"

service SuperNodeService {
  bool ping(),
  Address.Address Join(1: string IP, 2: i32 port),      //Node call this to Join DHT
  bool PostJoin(1: string IP, 2: i32 port),             //Node call this to notify finished joining DHT
  Address.Address GetNode(),                             //Client call this to get an initial node
  i32 GetPossibleKey(),
  i32 GetHashPara()
}