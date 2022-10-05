package org.example;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.postgresql.Driver;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.object.BatchSqlUpdate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final byte[] BLOB = prepareBlob();
    private static final String SQL = "INSERT INTO some_table(id, some_blob) VALUES (?, ?)";

    public static void main(String[] args) throws Exception {
        HikariConfig config = prepareConfig();
        try (HikariDataSource dataSource = new HikariDataSource(config)) {
            try (Connection connection = dataSource.getConnection()) {
                runBatchApacheQueryRunner(connection); // stuck
//                runBatchPureJdbc(connection); // works ok
//                runBatchJdbcTemplate(dataSource); // works ok
            }
        }
    }

    private static void runBatchPureJdbc(Connection connection) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(SQL, Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 0; i < 5; i++) {
                statement.setInt(1, i + 1);
                statement.setBytes(2, BLOB);
                statement.addBatch();
            }
            statement.executeBatch();

            try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                List<Entity> entities = getGeneratedKeys(generatedKeys);
                entities.forEach(entity -> System.out.println("Id = " + entity.id + ", bytes length = " + entity.blob.length));
            }
        }
    }

    private static void runBatchApacheQueryRunner(Connection connection) throws Exception {
        QueryRunner queryRunner = new QueryRunner();
        Object[][] params = new Object[5][2];

        for (int i = 0; i < 5; i++) {
            params[i][0] = i + 1;
            params[i][1] = BLOB;
        }

        List<Entity> entities = queryRunner.insertBatch(connection, SQL, Main::getGeneratedKeys, params);
        entities.forEach(entity -> System.out.println("Id = " + entity.id + ", bytes length = " + entity.blob.length));
    }

    private static void runBatchJdbcTemplate(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        KeyHolder keyHolder = new GeneratedKeyHolder();
        BatchSqlUpdate updateBatch = new BatchSqlUpdate();
        updateBatch.setSql(SQL);
        updateBatch.setJdbcTemplate(jdbcTemplate);
        updateBatch.setReturnGeneratedKeys(true);
        updateBatch.declareParameter(new SqlParameterValue(Types.INTEGER, "id"));
        updateBatch.declareParameter(new SqlParameterValue(Types.BINARY, "some_blob"));
        updateBatch.setBatchSize(5);

        for (int i = 0; i < 5; i++) {
            updateBatch.update(new Object[] { i + 1, BLOB }, keyHolder);
        }
        updateBatch.flush();
    }

    private static List<Entity> getGeneratedKeys(ResultSet resultSet) {
        try {
            List<Entity> result = new ArrayList<>();
            while (resultSet.next()) {
                int id = resultSet.getInt(1);
                byte[] blob = resultSet.getBytes(2);
                result.add(new Entity(id, blob));
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] prepareBlob() {
        try {
            InputStream inputStream = Main.class.getClassLoader().getResourceAsStream("10MB.txt");
            byte[] result = inputStream.readAllBytes();
            inputStream.close();
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static HikariConfig prepareConfig() {
        HikariConfig config = new HikariConfig();
        config.setUsername("user");
        config.setPassword("pass");
        config.setDriverClassName(Driver.class.getCanonicalName());
        config.setJdbcUrl("jdbc:postgresql://localhost:" + DbContainer.getPort() + "/dbname");
        return config;
    }
}

class Entity {
    int id;
    byte[] blob;

    Entity(int id, byte[] blob) {
        this.id = id;
        this.blob = blob;
    }
}

class DbContainer {
    static final PostgreSQLContainer<?> CONTAINER;

    static {
        CONTAINER = new PostgreSQLContainer<>("postgres:12")
                .withUsername("user")
                .withPassword("pass")
                .withDatabaseName("dbname")
                .withInitScript("init.sql");
        CONTAINER.start();
    }

    static int getPort() {
        return CONTAINER.getFirstMappedPort();
    }
}