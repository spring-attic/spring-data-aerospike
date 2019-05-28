package org.springframework.data.aerospike.core;

import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.domain.Sort;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

/**
 * Aerospike specific data access operations to work with reactive API
 *
 * @author Igor Ermolenko
 */
public interface ReactiveAerospikeOperations {
    <T> Mono<T> save(T document);

    <T> Flux<T> insertAll(Collection<? extends T> documents);

    <T> Mono<T> insert(T document);

    <T> Mono<T> update(T document);

    <T> Flux<T> findAll(Class<T> type);

    <T> Mono<Optional<T>> findById(Serializable id, Class<T> type);

    <T> Flux<T> findByIds(Iterable<?> ids, Class<T> type);

    <T> Flux<T> find(Query query, Class<T> type);

    <T> Flux<T> findInRange(long offset, long limit, Sort sort, Class<T> type);

    <T> Mono<Long> count(Query query, Class<T> type);
}
