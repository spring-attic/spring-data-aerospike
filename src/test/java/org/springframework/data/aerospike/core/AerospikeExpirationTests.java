package org.springframework.data.aerospike.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.DocumentWithDefaultConstructor;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpiration;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpirationAnnotation;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpirationOneDay;
import org.springframework.data.aerospike.SampleClasses.DocumentWithUnixTimeExpiration;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.google.common.collect.ImmutableMap;

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
    public void shouldAddValuesMapAndExpire() throws InterruptedException {
        DocumentWithDefaultConstructor document = new DocumentWithDefaultConstructor();
        document.setId(id);
        document.setExpiration(DateTime.now().plusSeconds(1));

        template.add(document, ImmutableMap.of("intField", 10L));
        Thread.sleep(500L);

        DocumentWithDefaultConstructor shouldNotExpire = template.findById(id, DocumentWithDefaultConstructor.class);
        assertThat(shouldNotExpire).isNotNull();
        assertThat(shouldNotExpire.getIntField()).isEqualTo(10);

        Thread.sleep(1500L);

        DocumentWithDefaultConstructor shouldExpire = template.findById(id, DocumentWithDefaultConstructor.class);
        assertThat(shouldExpire).isNull();
    }

    @Test
    public void shouldAddValueAndExpire() throws InterruptedException {
        DocumentWithDefaultConstructor document = new DocumentWithDefaultConstructor();
        document.setId(id);
        document.setExpiration(DateTime.now().plusSeconds(1));

        template.add(document, "intField", 10L);
        Thread.sleep(500L);

        DocumentWithDefaultConstructor shouldNotExpire = template.findById(id, DocumentWithDefaultConstructor.class);
        assertThat(shouldNotExpire).isNotNull();
        assertThat(shouldNotExpire.getIntField()).isEqualTo(10);

        Thread.sleep(1500L);

        DocumentWithDefaultConstructor shouldExpire = template.findById(id, DocumentWithDefaultConstructor.class);
        assertThat(shouldExpire).isNull();
    }

    @Test
    public void shouldExpireBasedOnUnixTimeValue() throws InterruptedException {
        template.insert(new DocumentWithUnixTimeExpiration(id, DateTime.now().plusSeconds(1)));

        Thread.sleep(500L);

        DocumentWithUnixTimeExpiration shouldNotExpire = template.findById(id, DocumentWithUnixTimeExpiration.class);
        assertThat(shouldNotExpire).isNotNull();

        Thread.sleep(1500L);

        DocumentWithUnixTimeExpiration shouldExpire = template.findById(id, DocumentWithUnixTimeExpiration.class);
        assertThat(shouldExpire).isNull();
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
    public void shouldUpdateExpirationOnTouchOnRead() throws InterruptedException {
        String id = nextId();
        template.insert(new DocumentWithExpirationOneDay(id));

        Key key = new Key(template.getNamespace(), template.getSetName(DocumentWithExpirationOneDay.class), id);

        Record record = template.getAerospikeClient().get(null, key);
        assertThat(record.getTimeToLive()).isCloseTo((int) TimeUnit.DAYS.toSeconds(1), offset(30));

        Thread.sleep(1000);
        template.findById(id, DocumentWithExpirationOneDay.class);

        record = template.getAerospikeClient().get(null, key);
        assertThat(record.getTimeToLive()).isCloseTo((int) TimeUnit.DAYS.toSeconds(1), offset(30));
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
