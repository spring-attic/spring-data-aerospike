package org.springframework.data.aerospike.mapping;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.*;
import org.springframework.data.mapping.PersistentPropertyAccessor;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.SampleClasses.*;
import static org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity.DEFAULT_EXPIRATION;

public class AerospikePersistentEntityTest extends BaseIntegrationTests {

    @Autowired
    private AerospikeMappingContext context;

    @Test
    public void shouldReturnExpirationForDocumentWithExpiration() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithExpiration.class);
        assertThat(persistentEntity.getExpiration()).isEqualTo(EXPIRATION);
    }

    @Test
    public void shouldReturnExpirationForDocumentWithExpirationExpression() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithExpirationExpression.class);
        assertThat(persistentEntity.getExpiration()).isEqualTo(EXPIRATION);
    }

    @Test
    public void shouldReturnExpirationForDocumentWithExpirationUnit() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithExpirationUnit.class);
        assertThat(persistentEntity.getExpiration()).isEqualTo((int) TimeUnit.MINUTES.toSeconds(1));
    }

    @Test
    public void shouldReturnZeroForDocumentWithoutExpiration() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithoutExpiration.class);
        assertThat(persistentEntity.getExpiration()).isEqualTo(DEFAULT_EXPIRATION);
    }

    @Test
    public void shouldReturnZeroForDocumentWithoutAnnotation() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithoutAnnotation.class);
        assertThat(persistentEntity.getExpiration()).isEqualTo(DEFAULT_EXPIRATION);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailForDocumentWithExpirationAndExpression() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithExpirationAndExpression.class);
        persistentEntity.getExpiration();
    }

    @Test
    public void shouldGetExpirationFromField() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithExpirationAnnotation.class);
        AerospikePersistentProperty expirationProperty = persistentEntity.getExpirationProperty();
        assertThat(expirationProperty).isNotNull();
        assertThat(expirationProperty.isExpirationProperty()).isTrue();

        DocumentWithExpirationAnnotation document = new DocumentWithExpirationAnnotation("docId", EXPIRATION);
        PersistentPropertyAccessor accessor = persistentEntity.getPropertyAccessor(document);
        assertThat(accessor.getProperty(expirationProperty)).isEqualTo(EXPIRATION);
    }
}
