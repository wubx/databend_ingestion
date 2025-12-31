package com.databend;

import com.databend.jdbc.DatabendConnection;
import com.databend.jdbc.DatabendConnectionExtension;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;

import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Operator;

public class Main {
   // private static final String URL = "jdbc:databend://192.168.126.31:8000/wubx";
   // private static final String USER = "databend";
   // private static final String PASSWORD = "databend";
    
    private static final String URL = "jdbc:databend://192.168.1.201:8000/wubx";
    private static final String USER = "root";
    private static final String PASSWORD = "";
    private static final String TABLE = "bench_insert";

    private static final long DEFAULT_TOTAL_ROWS = 50_000_000L;
    private static final int DEFAULT_BATCH_SIZE = 5_000;

    private enum Mode {
        INSERT,
        INSERT_NO_PRESIGN,
        STREAMING_LOAD,
        STAGE_LOAD
    }

    public static void main(String[] args) throws Exception {
        Mode mode = args.length > 0 ? Mode.valueOf(args[0].toUpperCase()) : Mode.INSERT;
        long totalRows = args.length > 1 ? Long.parseLong(args[1]) : DEFAULT_TOTAL_ROWS;
        int batchSize = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_BATCH_SIZE;

        System.out.printf("Mode=%s, totalRows=%d, batchSize=%d%n", mode, totalRows, batchSize);

        long start = System.currentTimeMillis();
        switch (mode) {
            case INSERT -> runInsert(totalRows, batchSize, false);
            case INSERT_NO_PRESIGN -> runInsert(totalRows, batchSize, true);
            case STREAMING_LOAD -> runStreamingLoad(totalRows, batchSize);
            case STAGE_LOAD -> runStageLoad(totalRows, batchSize);
        }
        long end = System.currentTimeMillis();
        double seconds = (end - start) / 1000.0;
        double rps = totalRows / seconds;
        System.out.printf("Finished mode=%s, totalRows=%d, cost=%.2fs, rows/s=%.0f%n",
                mode, totalRows, seconds, rps);
    }

    private static void ensureTable(Connection conn) throws SQLException {
        String ddl = """
                CREATE OR REPLACE TABLE bench_insert (
                    id BIGINT,
                    batch VARCHAR,
                    name VARCHAR,
                    birthday DATE,
                    address VARCHAR,
                    company VARCHAR,
                    job VARCHAR,
                    bank VARCHAR,
                    password VARCHAR,
                    phone_number VARCHAR,
                    user_agent VARCHAR,
                    c1 VARCHAR,
                    c2 VARCHAR,
                    c3 VARCHAR,
                    c4 VARCHAR,
                    c5 VARCHAR,
                    c6 VARCHAR,
                    c7 VARCHAR,
                    c8 VARCHAR,
                    c9 VARCHAR,
                    c10 VARCHAR,
                    d DATE,
                    t TIMESTAMP
                )
                """;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
        }
    }

    private static void ensureStage(Connection conn) throws SQLException {
        String ddl = """
                CREATE OR REPLACE STAGE d_stage
                URL='s3://v-wubx/wubx/'
                CONNECTION=(
                        endpoint_url = 'http://192.168.1.100:9001/'
                        region='us-east-1'
                        access_key_id = 'minioadmin'
                        secret_access_key = 'minioadmin'
                )
                """;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);
        }
    }

    private static void runInsert(long totalRows, int batchSize, boolean disablePresign) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        if (disablePresign) {
            props.setProperty("presigned_url_disabled", "true");
        }

        try (Connection conn = DriverManager.getConnection(URL, props)) {
            ensureTable(conn);
            conn.setAutoCommit(false);
            String sql = """
                    INSERT INTO bench_insert (
                        id, batch, name, birthday, address, company, job, bank, password,
                        phone_number, user_agent, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, d, t
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?
                    )
                    """;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                long written = 0;
                long batchNo = 0;
                long dbNanos = 0;
                while (written < totalRows) {
                    int currentBatchSize = (int) Math.min(batchSize, totalRows - written);
                    List<Map<String, Object>> rows = GenerateData.genBatch("batch_" + batchNo, currentBatchSize);
                    for (Map<String, Object> row : rows) {
                        int idx = 1;
                        ps.setLong(idx++, (Integer) row.get("id"));
                        ps.setString(idx++, (String) row.get("batch"));
                        ps.setString(idx++, (String) row.get("name"));
                        ps.setString(idx++, (String) row.get("birthday"));
                        ps.setString(idx++, (String) row.get("address"));
                        ps.setString(idx++, (String) row.get("company"));
                        ps.setString(idx++, (String) row.get("job"));
                        ps.setString(idx++, (String) row.get("bank"));
                        ps.setString(idx++, (String) row.get("password"));
                        ps.setString(idx++, (String) row.get("phone_number"));
                        ps.setString(idx++, (String) row.get("user_agent"));
                        ps.setString(idx++, (String) row.get("c1"));
                        ps.setString(idx++, (String) row.get("c2"));
                        ps.setString(idx++, (String) row.get("c3"));
                        ps.setString(idx++, (String) row.get("c4"));
                        ps.setString(idx++, (String) row.get("c5"));
                        ps.setString(idx++, (String) row.get("c6"));
                        ps.setString(idx++, (String) row.get("c7"));
                        ps.setString(idx++, (String) row.get("c8"));
                        ps.setString(idx++, (String) row.get("c9"));
                        ps.setString(idx++, (String) row.get("c10"));
                        ps.setString(idx++, (String) row.get("d"));
                        ps.setString(idx, (String) row.get("t"));
                        ps.addBatch();
                    }
                    long batchStart = System.nanoTime();
                    ps.executeBatch();
                    long batchEnd = System.nanoTime();
                    dbNanos += (batchEnd - batchStart);
                    //conn.commit();
                    written += currentBatchSize;
                    batchNo++;
                    if (batchNo % 10 == 0) {
                        System.out.printf("INSERT%s: written=%d rows%n",
                                disablePresign ? "_NO_PRESIGN" : "", written);
                    }
                }
                if (dbNanos > 0) {
                    double seconds = dbNanos / 1_000_000_000.0;
                    double rps = written / seconds;
                    System.out.printf("INSERT%s_DB_ONLY: totalRows=%d, dbCost=%.2fs, rows/s=%.0f%n",
                            disablePresign ? "_NO_PRESIGN" : "",
                            written,
                            seconds,
                            rps);
                }
            }
        }
    }

    private static AsyncOperator createS3Operator() {
        Map<String, String> conf = new HashMap<>();
        conf.put("bucket", "v-wubx");
        conf.put("root", "wubx");
        conf.put("endpoint", "http://192.168.1.100:9001/");
        conf.put("region", "us-east-01");
        conf.put("access_key_id", "minioadmin");
        conf.put("secret_access_key", "minioadmin");
        conf.put("enable_virtual_host_style", "false");

        return AsyncOperator.of("s3", conf);
    }

    private static void runStreamingLoad(long totalRows, int batchSize) throws Exception {
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);

        try (Connection conn = DriverManager.getConnection(URL, props)) {
            ensureTable(conn);
            DatabendConnection databendConnection = conn.unwrap(DatabendConnection.class);
            long written = 0;
            long batchNo = 0;
            long dbNanos = 0;
            while (written < totalRows) {
                int currentBatchSize = (int) Math.min(batchSize, totalRows - written);
                List<Map<String, Object>> rows = GenerateData.genBatch("batch_" + batchNo, currentBatchSize);
                byte[] payload = buildCsvPayload(rows);
                try (InputStream in = new ByteArrayInputStream(payload)) {
                    // use special stage `_databend_load`, LoadMethod.STAGE
                    String sql = "INSERT INTO " + TABLE + " FROM @_databend_load FILE_FORMAT=(type=CSV)";
                    long batchStart = System.nanoTime();
                    int loaded = databendConnection.loadStreamToTable(
                            sql,
                            in,
                            payload.length,
                            DatabendConnectionExtension.LoadMethod.STREAMING
                    );
                    long batchEnd = System.nanoTime();
                    dbNanos += (batchEnd - batchStart);
                    written += loaded;
                }
                batchNo++;
                if (batchNo % 10 == 0) {
                    System.out.printf("STREAMING_LOAD: written=%d rows%n", written);
                }
            }
            if (dbNanos > 0) {
                double seconds = dbNanos / 1_000_000_000.0;
                double rps = written / seconds;
                System.out.printf("STREAMING_LOAD_DB_ONLY: totalRows=%d, dbCost=%.2fs, rows/s=%.0f%n",
                        written,
                        seconds,
                        rps);
            }
        }
    }

    private static void runStageLoad(long totalRows, int batchSize) throws Exception {
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);

        try (Connection conn = DriverManager.getConnection(URL, props);
             AsyncOperator op = createS3Operator()) {
            ensureStage(conn);
            ensureTable(conn);
            long generated = 0;
            long batchNo = 0;
            long writeNanos = 0;
            while (generated < totalRows) {
                int currentBatchSize = (int) Math.min(batchSize, totalRows - generated);
                List<Map<String, Object>> rows = GenerateData.genBatch("batch_" + batchNo, currentBatchSize);
                byte[] payload = buildCsvPayload(rows);
                String path = "batch_" + batchNo + ".csv";
                long writeStart = System.nanoTime();
                op.write(path, payload).join();
                long writeEnd = System.nanoTime();
                writeNanos += (writeEnd - writeStart);
                generated += currentBatchSize;
                batchNo++;
                if (batchNo % 10 == 0) {
                    System.out.printf("STAGE_LOAD_WRITE: batches=%d, generated=%d rows%n", batchNo, generated);
                }
            }

            String copySql = "COPY INTO " + TABLE +
                    " FROM @d_stage PATTERN='.*[.]csv' FILE_FORMAT=(type=CSV) PURGE=TRUE";
            long copyStart = System.nanoTime();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(copySql);
            }
            long copyEnd = System.nanoTime();

            double writeSeconds = writeNanos / 1_000_000_000.0;
            double copySeconds = (copyEnd - copyStart) / 1_000_000_000.0;
            double totalSeconds = writeSeconds + copySeconds;
            double copyRps = copySeconds > 0 ? totalRows / copySeconds : 0;

            System.out.printf(
                    "STAGE_LOAD: totalRows=%d, batches=%d, writeCost=%.2fs, copyCost=%.2fs, totalCost=%.2fs, copyRows/s=%.0f%n",
                    totalRows,
                    batchNo,
                    writeSeconds,
                    copySeconds,
                    totalSeconds,
                    copyRps);
        }
    }

    private static byte[] buildCsvPayload(List<Map<String, Object>> rows) {
        StringBuilder sb = new StringBuilder(rows.size() * 128);
        for (Map<String, Object> row : rows) {
            appendCsvField(sb, row.get("id"));
            sb.append(',');
            appendCsvField(sb, row.get("batch"));
            sb.append(',');
            appendCsvField(sb, row.get("name"));
            sb.append(',');
            appendCsvField(sb, row.get("birthday"));
            sb.append(',');
            appendCsvField(sb, row.get("address"));
            sb.append(',');
            appendCsvField(sb, row.get("company"));
            sb.append(',');
            appendCsvField(sb, row.get("job"));
            sb.append(',');
            appendCsvField(sb, row.get("bank"));
            sb.append(',');
            appendCsvField(sb, row.get("password"));
            sb.append(',');
            appendCsvField(sb, row.get("phone_number"));
            sb.append(',');
            appendCsvField(sb, row.get("user_agent"));
            sb.append(',');
            appendCsvField(sb, row.get("c1"));
            sb.append(',');
            appendCsvField(sb, row.get("c2"));
            sb.append(',');
            appendCsvField(sb, row.get("c3"));
            sb.append(',');
            appendCsvField(sb, row.get("c4"));
            sb.append(',');
            appendCsvField(sb, row.get("c5"));
            sb.append(',');
            appendCsvField(sb, row.get("c6"));
            sb.append(',');
            appendCsvField(sb, row.get("c7"));
            sb.append(',');
            appendCsvField(sb, row.get("c8"));
            sb.append(',');
            appendCsvField(sb, row.get("c9"));
            sb.append(',');
            appendCsvField(sb, row.get("c10"));
            sb.append(',');
            appendCsvField(sb, row.get("d"));
            sb.append(',');
            appendCsvField(sb, row.get("t"));
            sb.append('\n');
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void appendCsvField(StringBuilder sb, Object value) {
        if (value == null) {
            return;
        }
        String s = value.toString();
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                sb.append("\"\"");
            } else {
                sb.append(c);
            }
        }
        sb.append('"');
    }
}
