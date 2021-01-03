include "Address.thrift"

service NodeService {
    string Read(1: string FileName),
    //client call Read() to read a file
    i32 Write(1: string FileName, 2: string FileContent),
    //client call Write() to write a file
    map<string,i32> lsDir(),
    //client call Write() to write a file
    i32 GetVersion(1: string FileName),
    //coordinator call this to get the version of file on this server
    string DirectRead(1: string FileName),
    //a coordinator call DirectRead() to read file
    i32 DirectWrite(1: string FileName, 2: string FileContent, 3: i32 FileNewVer),
    //a coordinator call DirectWrite() to write file
    bool Coord_reset(1: i32 nr, 2: i32 nw)
    //client call Coord_reset() to reset parameters on coordinator
}