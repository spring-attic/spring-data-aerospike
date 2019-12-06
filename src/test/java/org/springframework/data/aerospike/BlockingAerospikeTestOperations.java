package org.springframework.data.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import lombok.RequiredArgsConstructor;
import org.awaitility.Awaitility;
import org.springframework.data.aerospike.core.AerospikeTemplate;

import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@RequiredArgsConstructor
public class BlockingAerospikeTestOperations {

    private final AerospikeTemplate template;
    private final AerospikeClient client;

    public void deleteAll(Class... entityClasses) {
        Arrays.asList(entityClasses).forEach(template::delete);
        Arrays.asList(entityClasses).forEach(this::awaitUntilSetIsEmpty);
    }

    @SuppressWarnings("unchecked")
    private void awaitUntilSetIsEmpty(Class entityClass) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .until(() -> isEmptySet(client, template.getNamespace(), entityClass));
    }

    public <T> boolean isEmptySet(IAerospikeClient client, String namespace, Class<T> entityClass) {
        String answer = Info.request(client.getNodes()[0], "sets/" + namespace + "/" + template.getSetName(entityClass));
        return answer.isEmpty()
                || Stream.of(answer.split(";")).allMatch(s -> s.contains("objects=0"));
    }

    public <T> void createIndexIfNotExists(Class<T> entityClass, String indexName, String binName, IndexType indexType) {
        ignoreError(ResultCode.INDEX_ALREADY_EXISTS,
                () -> wait(client.createIndex(null, template.getNamespace(), template.getSetName(entityClass), indexName, binName, indexType)));
    }

    public <T> void dropIndexIfExists(Class<T> entityClass, String indexName) {
        ignoreError(ResultCode.INDEX_NOTFOUND,
                () -> wait(client.dropIndex(null, template.getNamespace(), template.getSetName(entityClass), indexName)));
    }

    // Do not use this code in production!
    // This will not guarantee the correct answer from Aerospike Server for all cases.
    // Also it requests index status only from one Aerospike node, which is OK for tests, and NOT OK for Production cluster.
    public boolean indexExists(String indexName) {
        Node[] nodes = client.getNodes();
        if (nodes.length == 0) {
            throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
        }
        Node node = nodes[0];
        String response = Info.request(node, "sindex/" + template.getNamespace() + '/' + indexName);
        return !response.startsWith("FAIL:201");
    }

    public void addNewFieldToSavedDataInAerospike(Key key) {
        Record initial = client.get(new Policy(), key);
        Bin[] bins = Stream.concat(
                initial.bins.entrySet().stream().map(e -> new Bin(e.getKey(), e.getValue())),
                Stream.of(new Bin("notPresent", "cats"))).toArray(Bin[]::new);
        WritePolicy policy = new WritePolicy();
        policy.recordExistsAction = RecordExistsAction.REPLACE;

        client.put(policy, key, bins);

        Record updated = client.get(new Policy(), key);
        assertThat(updated.bins.get("notPresent")).isEqualTo("cats");
    }

    private static void wait(IndexTask task) {
        if (task == null) {
            throw new IllegalStateException("task can not be null");
        }
        task.waitTillComplete();
    }

    private void ignoreError(int errorCodeToSkip, Runnable runnable) {
        try {
            runnable.run();
        } catch (AerospikeException e) {
            if (e.getResultCode() != errorCodeToSkip) {
                throw e;
            }
        }
    }

}
