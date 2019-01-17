package com.dz.R2DBC;

import io.r2dbc.client.R2dbc;
import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class R2DbcApplication {

    public static void main(String[] args) {
        SpringApplication.run(R2DbcApplication.class, args);
    }


    @Component
    public class Config {

        @PostConstruct
        public void init() {

            H2ConnectionConfiguration config = H2ConnectionConfiguration.builder()
                    .url("jdbc:h2:mem:testdb")
                    .username("sa")
                    .file("./file")
                    .password("")
                    .build();

            R2dbc client = new R2dbc(new H2ConnectionFactory(config));

            Mono<Void> createTable = client
                    .inTransaction(
                            handle -> handle.execute("CREATE TABLE IF NOT EXISTS test ( " +
                                    "id INT, " +
                                    "name VARCHAR(255)" +
                                    ")"))
                    .then()
                    .doOnSuccess(v -> System.out.println("Table created"));

            Mono<Void> insertValues = Flux
                    .range(0,100)
                    .flatMap(i -> client
                            .inTransaction(handle -> handle
                                    .execute("INSERT INTO test VALUES ($1, $2)", i, "Name: " + i)))
                    .then()
                    .doOnSuccess(v -> System.out.println("Values inserted"));

            Mono<Void> read = client
                    .open()
                    .flatMap(handle -> handle
                            .select("SELECT * from test")
                            .mapResult(result -> result.map((row, rowMetadata) -> row.get("name")))
                            .cast(String.class)
                            .doOnNext(System.out::println)
                            .then()

                    )
                    .then()
                    .doOnSuccess(v -> System.out.println("Data read"));


            createTable
                    .then(insertValues)
                    .then(read)
                    .doOnSuccess(v -> System.out.println("All done"))
                    .subscribe();
        }
    }

}

