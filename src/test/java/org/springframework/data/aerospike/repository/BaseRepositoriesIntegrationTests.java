package org.springframework.data.aerospike.repository;

import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.aerospike.config.TestConfig;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = TestConfig.class)
public abstract class BaseRepositoriesIntegrationTests {
}
