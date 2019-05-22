package org.springframework.data.aerospike.core;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.helper.query.QueryEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.convert.*;
import org.springframework.data.aerospike.mapping.*;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.util.Assert;

import java.util.Collections;

/**
 * Base class for creation Aerospike templates
 *
 * @author Igor Ermolenko
 */
abstract class BaseAerospikeTemplate {

    protected final MappingContext<BasicAerospikePersistentEntity<?>, AerospikePersistentProperty> mappingContext;
    protected final AerospikeClient client;
    protected final MappingAerospikeConverter converter;
    protected final String namespace;
    protected final QueryEngine queryEngine;
    protected final AerospikeExceptionTranslator exceptionTranslator;

    BaseAerospikeTemplate(AerospikeClient client,
                          String namespace,
                          MappingAerospikeConverter converter,
                          AerospikeMappingContext mappingContext,
                          AerospikeExceptionTranslator exceptionTranslator) {
        Assert.notNull(client, "Aerospike client must not be null!");
        Assert.notNull(namespace, "Namespace cannot be null");
        Assert.hasLength(namespace, "Namespace cannot be empty");

        this.client = client;
        this.converter = converter;
        this.exceptionTranslator = exceptionTranslator;
        this.namespace = namespace;
        this.mappingContext = mappingContext;
        this.queryEngine = new QueryEngine(this.client);

        loggerSetup();
    }

    BaseAerospikeTemplate(AerospikeClient client, String namespace) {
        Assert.notNull(client, "Aerospike client must not be null!");
        Assert.notNull(namespace, "Namespace cannot be null");
        Assert.hasLength(namespace, "Namespace cannot be empty");

        CustomConversions customConversions = new CustomConversions(Collections.emptyList(), AerospikeSimpleTypes.HOLDER);
        AerospikeMappingContext asContext = new AerospikeMappingContext();
        asContext.setDefaultNameSpace(namespace);

        this.client = client;
        this.converter = new MappingAerospikeConverter(asContext, customConversions, new AerospikeTypeAliasAccessor());
        this.exceptionTranslator = new DefaultAerospikeExceptionTranslator();
        this.namespace = namespace;
        this.mappingContext = asContext;
        this.queryEngine = new QueryEngine(this.client);

        this.converter.afterPropertiesSet();

        loggerSetup();
    }

    private void loggerSetup() {
        final Logger log = LoggerFactory.getLogger("com.aerospike.client");
        com.aerospike.client.Log
                .setCallback((level, message) -> {
                    switch (level) {
                        case INFO:
                            log.info("{}", message);
                            break;
                        case DEBUG:
                            log.debug("{}", message);
                            break;
                        case ERROR:
                            log.error("{}", message);
                            break;
                        case WARN:
                            log.warn("{}", message);
                            break;
                    }

                });
    }

    <T> T mapToEntity(Key key, Class<T> type, Record record) {
        if (record == null) {
            return null;
        }
        AerospikeReadData data = AerospikeReadData.forRead(key, record);
        T readEntity = converter.read(type, data);

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(type);
        if (entity.hasVersionProperty()) {
            final ConvertingPropertyAccessor accessor = getPropertyAccessor(entity, readEntity);
            accessor.setProperty(entity.getVersionProperty(), record.generation);
        }

        return readEntity;
    }

    <T> ConvertingPropertyAccessor<T> getPropertyAccessor(AerospikePersistentEntity<?> entity, T source) {
        PersistentPropertyAccessor<T> accessor = entity.getPropertyAccessor(source);
        return new ConvertingPropertyAccessor<T>(accessor, converter.getConversionService());
    }

    WritePolicy getCasAwareWritePolicy(AerospikeWriteData data, AerospikePersistentEntity<?> entity,
                                               ConvertingPropertyAccessor<?> accessor) {
        WritePolicyBuilder builder = WritePolicyBuilder.builder(this.client.writePolicyDefault)
                .sendKey(true)
                .generationPolicy(GenerationPolicy.EXPECT_GEN_EQUAL)
                .expiration(data.getExpiration());

        Integer version = accessor.getProperty(entity.getVersionProperty(), Integer.class);
        boolean existingDocument = version != null && version > 0L;
        if (existingDocument) {
            //Updating existing document with generation
            builder.recordExistsAction(RecordExistsAction.REPLACE_ONLY)
                    .generation(version);
        } else {
            // create new document. if exists we should fail with optimistic locking
            builder.recordExistsAction(RecordExistsAction.CREATE_ONLY);
        }

        return builder.build();
    }

    Key getKey(Object id, AerospikePersistentEntity<?> entity) {
        return new Key(this.namespace, entity.getSetName(), id.toString());
    }


}
