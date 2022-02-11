package io.confluent.parallelconsumer.state;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;

class ShardManagerTest {
    @Test
    void concurrent() {
        var m = new ConcurrentHashMap<>();
        m.put(1, 1);
        m.put(2, 2);
        var iterator = m.entrySet().iterator();
        m.clear();
        var next1 = iterator.next();
//        m.remove(2);
        var next2 = iterator.next();
        var next3 = iterator.next();
    }
}
