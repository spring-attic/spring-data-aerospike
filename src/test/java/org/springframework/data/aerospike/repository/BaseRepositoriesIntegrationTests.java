package org.springframework.data.aerospike.repository;

import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.aerospike.EmbeddedAerospikeInfo;
import org.springframework.data.aerospike.config.TestConfig;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.atomic.AtomicLong;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = TestConfig.class)
public abstract class BaseRepositoriesIntegrationTests {

    private static AtomicLong counter = new AtomicLong();

    @Autowired
    protected EmbeddedAerospikeInfo info;

    protected static String nextId() {
        return "as-" + counter.incrementAndGet();
    }
}
