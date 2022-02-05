package pl.edu.put.concurrent.jiffy;

class NewRevisionWithContext<K,V> {
    Revision<K,V> revision;
    Revision<K,V> head;
    int index;

    NewRevisionWithContext(Revision<K,V> revision, Revision<K,V> head, int index) {
        this.revision = revision;
        this.head = head;
        this.index = index;
    }
}
