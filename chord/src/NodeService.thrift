include "Address.thrift"

service NodeService {
  bool ping(),
  string Set(1: string BookTitle, 2: string Genre),         //client call this to add element in DHT
  string Get(1: string BookTitle),                       //client call this to get element in DHT
  void UpdateDHT(1: Address.Address NN, 2: i32 ID, 3: list<i64> chain),      //node call this to update this.FingerTable

  Address.Address GetPredecessor(),
  Address.Address GetSuccessor(),
  void SetPredecessor(1: Address.Address _a),
  void SetSuccessor(1: Address.Address _a),

  Address.Address FindClosetPrecedingFinger(1: i64 MID)
  Address.Address FindSuccessor(1: i64 MID, 2: list<i64> chain)
  Address.Address FindPredecessor(1: i64 MID, 2: list<i64> chain)
}