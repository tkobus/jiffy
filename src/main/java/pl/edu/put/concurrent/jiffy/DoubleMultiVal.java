package pl.edu.put.concurrent.jiffy;

class DoubleMultiVal<K,V> {
    MultiVal<K,V> left;
    MultiVal<K,V> right;

    DoubleMultiVal(MultiVal<K,V> left, MultiVal<K,V> right) {
        this.left = left;
        this.right = right;
    }
}
