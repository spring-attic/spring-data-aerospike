package org.springframework.data.aerospike.core;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpiration;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpirationAnnotation;
import org.springframework.data.aerospike.SampleClasses.DocumentWithTouchOnRead;

import static org.assertj.core.api.Assertions.assertThat;

//TODO: Potentially unstable tests. Instead of sleeping, we need somehow do time travel like in CouchbaseMock.
public class AerospikeExpirationTests extends BaseIntegrationTests {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Autowired
    private AerospikeTemplate template;

    private String id;

    @Before
    public void setUp() throws Exception {
        this.id = nextId();
    }

    @Test
    public void shouldExpireBasedOnFieldValue() throws InterruptedException {
        template.insert(new DocumentWithExpirationAnnotation(id, 1));

        Thread.sleep(500L);

        DocumentWithExpirationAnnotation shouldNotExpire = template.findById(id, DocumentWithExpirationAnnotation.class);
        assertThat(shouldNotExpire).isNotNull();

        Thread.sleep(1500L);

        DocumentWithExpirationAnnotation shouldExpire = template.findById(id, DocumentWithExpirationAnnotation.class);
        assertThat(shouldExpire).isNull();
    }

    @Test
    public void shouldReturnExpirationValue() throws InterruptedException {
        template.insert(new DocumentWithExpirationAnnotation(id, 5));

        Thread.sleep(1500L);

        DocumentWithExpirationAnnotation document = template.findById(id, DocumentWithExpirationAnnotation.class);
        assertThat(document.getExpiration()).isGreaterThan(0).isLessThan(5);
    }

    @Test
    public void shouldNotExpireWhenTouchOnRead() throws InterruptedException {
        String id = nextId();
        template.insert(new DocumentWithTouchOnRead(id));

        Thread.sleep(500L);

        DocumentWithTouchOnRead shouldNotExpire = template.findById(id, DocumentWithTouchOnRead.class);
        assertThat(shouldNotExpire).isNotNull();

        Thread.sleep(1500L);

        shouldNotExpire = template.findById(id, DocumentWithTouchOnRead.class);
        assertThat(shouldNotExpire).isNotNull();
    }

    @Test
    public void shouldExpire() throws Exception {
        template.insert(new DocumentWithExpiration(id));

        Thread.sleep(500L);

        DocumentWithExpiration shouldNotExpire = template.findById(id, DocumentWithExpiration.class);
        assertThat(shouldNotExpire).isNotNull();

        Thread.sleep(1500L);

        DocumentWithExpiration shouldExpire = template.findById(id, DocumentWithExpiration.class);
        assertThat(shouldExpire).isNull();
    }
}
