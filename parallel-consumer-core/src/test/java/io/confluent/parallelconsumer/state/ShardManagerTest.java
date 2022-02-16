package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;

// todo remove
class ShardManagerTest {
    @Test
    void concurrent() {
        var m = new ConcurrentHashMap<>();
        m.put(1, 1);
        m.put(2, 2);
        var iterator = m.entrySet().iterator();
        var next1 = iterator.next();
        m.remove(2);
        var next2 = iterator.next();
    }
}
