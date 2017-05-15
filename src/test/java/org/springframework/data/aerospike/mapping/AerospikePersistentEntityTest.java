package org.springframework.data.aerospike.mapping;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.repository.BaseRepositoriesIntegrationTests;

import java.util.concurrent.TimeUnit;

import static org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity.DEFAULT_EXPIRATION;

public class AerospikePersistentEntityTest extends BaseRepositoriesIntegrationTests {

    @Autowired
    private AerospikeMappingContext context;

    @Test
    public void shouldReturnExpirationForDocumentWithExpiry() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithExpiry.class);
        Assertions.assertThat(persistentEntity.getExpiration()).isEqualTo(42);
    }

    @Test
    public void shouldReturnExpirationForDocumentWithExpiryExpression() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithExpiryExpression.class);
        Assertions.assertThat(persistentEntity.getExpiration()).isEqualTo(42);
    }

    @Test
    public void shouldReturnExpirationForDocumentWithExpiryUnit() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithExpiryUnit.class);
        Assertions.assertThat(persistentEntity.getExpiration()).isEqualTo((int) TimeUnit.MINUTES.toSeconds(1));
    }

    @Test
    public void shouldReturnZeroForDocumentWithoutExpiry() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithoutExpiry.class);
        Assertions.assertThat(persistentEntity.getExpiration()).isEqualTo(DEFAULT_EXPIRATION);
    }

    @Test
    public void shouldReturnZeroForDocumentWithoutAnnotation() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithoutAnnotation.class);
        Assertions.assertThat(persistentEntity.getExpiration()).isEqualTo(DEFAULT_EXPIRATION);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailForDocumentWithExpiryAndExpiryExpression() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithExpiryAndExpiryExpression.class);
        persistentEntity.getExpiration();
    }

    @Document(expiry = 42)
    public static class DocumentWithExpiry {

    }

    @Document(expiryExpression = "${expirationProperty}")
    public static class DocumentWithExpiryExpression {

    }

    @Document(expiry = 1, expiryUnit = TimeUnit.MINUTES)
    public static class DocumentWithExpiryUnit {

    }

    @Document
    public static class DocumentWithoutExpiry {

    }

    public static class DocumentWithoutAnnotation {

    }

    @Document(expiry = 1, expiryExpression = "${expirationProperty}")
    public static class DocumentWithExpiryAndExpiryExpression {

    }
}
