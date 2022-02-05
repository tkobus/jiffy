package pl.edu.put.concurrent.jiffy;

class NodeWithContext<K,V> {
    Node<K,V> node;
    Revision<K,V> revision;
    int index;
    Node<K,V> nextNode;

    NodeWithContext(Node<K,V> node, Revision<K,V> revision, int index, Node<K,V> nextNode) {
        this.node = node;
        this.revision = revision;
        this.index = index;
        this.nextNode = nextNode;
    }
}
