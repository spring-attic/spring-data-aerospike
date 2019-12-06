package org.springframework.data.aerospike.core;

import com.aerospike.client.query.IndexType;
import lombok.Value;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.aerospike.AsyncUtils;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.IndexAlreadyExistsException;
import org.springframework.data.aerospike.IndexNotFoundException;
import org.springframework.data.aerospike.mapping.Document;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.AwaitilityUtils.awaitTenSecondsUntil;

public class AerospikeTemplateIndexTests extends BaseBlockingIntegrationTests {

    private static final String INDEX_TEST_1 = "index-test-77777";

    @Override
    @Before
    public void setUp() {
        blockingAerospikeTestOperations.dropIndexIfExists(IndexedDocument.class, INDEX_TEST_1);
    }

    @Test
    public void createIndex_createsIndexIfExecutedConcurrently() throws Exception {
        AtomicInteger errors = new AtomicInteger();
        AsyncUtils.executeConcurrently(5, () -> {
            try {
                template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING);
            } catch (IndexAlreadyExistsException e) {
                errors.incrementAndGet();
            }
        });

        awaitTenSecondsUntil(() ->
                assertThat(blockingAerospikeTestOperations.indexExists(INDEX_TEST_1)).isTrue());
        assertThat(errors.get()).isLessThanOrEqualTo(4);// depending on the timing all 5 requests can succeed on Aerospike Server
    }

    @Test
    public void createIndex_allCreateIndexConcurrentAttemptsFailIfIndexAlreadyExists() throws Exception {
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING);

        awaitTenSecondsUntil(() ->
                assertThat(blockingAerospikeTestOperations.indexExists(INDEX_TEST_1)).isTrue());

        AtomicInteger errors = new AtomicInteger();
        AsyncUtils.executeConcurrently(5, () -> {
            try {
                template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING);
            } catch (IndexAlreadyExistsException e) {
                errors.incrementAndGet();
            }
        });

        assertThat(errors.get()).isEqualTo(5);
    }

    @Test
    public void createIndex_createsIndex() {
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING);

        awaitTenSecondsUntil(() ->
                assertThat(blockingAerospikeTestOperations.indexExists(INDEX_TEST_1)).isTrue());
    }

    @Test
    public void createIndex_throwsExceptionIfIndexAlreadyExists() {
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING);

        awaitTenSecondsUntil(() -> assertThat(blockingAerospikeTestOperations.indexExists(INDEX_TEST_1)).isTrue());

        assertThatThrownBy(() -> template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING))
                .isInstanceOf(IndexAlreadyExistsException.class);
    }

    @Test
    public void deleteIndex_throwsExceptionIfIndexDoesNotExist() {
        assertThatThrownBy(() -> template.deleteIndex(IndexedDocument.class, "not-existing-index"))
                .isInstanceOf(IndexNotFoundException.class);
    }

    @Test
    public void deleteIndex_deletesExistingIndex() {
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING);

        awaitTenSecondsUntil(() -> assertThat(blockingAerospikeTestOperations.indexExists(INDEX_TEST_1)).isTrue());

        template.deleteIndex(IndexedDocument.class, INDEX_TEST_1);

        awaitTenSecondsUntil(() -> assertThat(blockingAerospikeTestOperations.indexExists(INDEX_TEST_1)).isFalse());
    }

    @Value
    @Document
    public static class IndexedDocument {

        String stringField;
        int intField;
    }
}
