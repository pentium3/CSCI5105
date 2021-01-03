include "Address.thrift"

service CoordinatorService {
  string Coord_Read(1: string filename),
  bool Coord_Write(1: string filename, 2: string fileContent),
  void sync(),
  map<string, i32> Coord_lsDir(),
  bool Join(1: Address.Address address),
  bool reset(1: i32 nr, 2: i32 nw),

    string Read(1: string FileName),
    //client call Read() to read a file
    i32 Write(1: string FileName, 2: string FileContent),
    //client call Write() to write a file
    map<string,i32> lsDir(),
    //client call Write() to write a file
    bool Coord_reset(1: i32 nr, 2: i32 nw)
    //client call Coord_reset() to reset parameters on coordinator
}
