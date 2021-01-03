public class FingerItem{    //For FingerTable[i],
    Address Node;           //  Node = Successor(FingerTable[i].start)
    long ivx; long ivy;     //  [ivx, ivy) = [FingerTable[i].start, FingerTable[i+1].start)
    long start;             //  start = (NodeInfo.ID+(long)(Math.pow(2,i-1))) % (long)(Math.pow(2,ChordLen)))
    public FingerItem(Address _Node, long _ivx, long _ivy, long _start) {
        Node=_Node;
        ivx=_ivx;
        ivy=_ivy;
        start=_start;
    }
    public static long FingerCalc(long _n, int _k, int _m) {
        if (_k == _m + 1)
            return (_n);
        else
            return ((_n + (long) (Math.pow(2, _k - 1))) % (long) (Math.pow(2, _m)));
    }
    public void PrintItem(int i){
        System.out.println("FingerItem"+i+" "+Node+" "+ivx+"_"+ivy+" : "+start);
    }
}