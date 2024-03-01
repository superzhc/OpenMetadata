package com.xgit.openmetadata.client;

import org.junit.Test;
import org.openmetadata.client.gateway.OpenMetadata;
import org.openmetadata.schema.security.client.OpenMetadataJWTClientConfig;
import org.openmetadata.schema.services.connections.metadata.AuthProvider;
import org.openmetadata.schema.services.connections.metadata.OpenMetadataConnection;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class OpenMetadataBaseTest {
    private static final String JWT_TOKEN =
            "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6InNkay11c2FnZSIsImVtYWlsIjoic2RrLXVzYWdlQHhnaXQuY29tIiwiaXNCb3QiOnRydWUsInRva2VuVHlwZSI6IkJPVCIsImlhdCI6MTcwNjA4MjU5MiwiZXhwIjpudWxsfQ.DkYURjMmcl86T1eLzHxtScDDIeNw8w1cdp-cQ_ZRh8BVmLd1fUtD6KOypvcN-4I3K1p-r2FkEAwvh6H4qT03pctXnENJ7aiZHowKOvaWMQ2W5oJDPUXK5w73--M7G7XvB1C2DcFHYfIqjYGQejooZ-ex-f7e0YrZBpAsXlbtYPHXMrlzi66gEbgb4xbNJN3p4afmxdSP8yua3_X_3uf98o_Y_A9tIkJFNdxSxbRQwi62OSPXpKv8alhCGYNp9917n-9IVFCMzYc3hzOrqoZJrcgMo6jdXI5MkIZM0_iGiHrnxmpbj_xhbft1dtr035sQbdZh8Qu5jV7nWQmmScJRZQ";
    private static final String HOST_PORT = "http://10.90.20.236:8585/api";

    private OpenMetadata openMetadata = null;

    private LocalDateTime current;

    public OpenMetadataBaseTest() {
        current = LocalDateTime.now();
    }

    protected OpenMetadataConnection openMetadataConnection() {
        OpenMetadataConnection connection = new OpenMetadataConnection();
        setHostPort(connection);
        setAuth(connection);

        return connection;
    }

    protected OpenMetadataConnection setHostPort(final OpenMetadataConnection openMetadataConnection) {
        openMetadataConnection.setHostPort(HOST_PORT);
        return openMetadataConnection;
    }

    protected OpenMetadataConnection setAuth(final OpenMetadataConnection openMetadataConnection) {
        OpenMetadataJWTClientConfig openMetadataJWTClientConfig = new OpenMetadataJWTClientConfig();
        openMetadataJWTClientConfig.setJwtToken(JWT_TOKEN);
        openMetadataConnection.setSecurityConfig(openMetadataJWTClientConfig);

        openMetadataConnection.setAuthProvider(AuthProvider.OPENMETADATA);

        return openMetadataConnection;
    }

    public OpenMetadata getClient() {
        if (openMetadata == null) {
            openMetadata = new OpenMetadata(openMetadataConnection());
        }

        return openMetadata;
    }

    //region 工具方法
    public String year() {
        return currentFormat("yyyy");
    }

    public String month(){
        return currentFormat("yyyyMM");
    }

    public String day(){
        return currentFormat("yyyyMMdd");
    }

    public String hour(){
        return currentFormat("yyyyMMddHH");
    }

    public String minute(){
        return currentFormat("yyyyMMddHHmm");
    }

    public String currentFormat(String format) {
        if (null == format || format.trim().length() == 0) {
            format = "yyyyMMddHHmmssSSS";
        }
        return current.format(DateTimeFormatter.ofPattern(format));
    }
    //endregion

    @Test
    public void testVersion() {
        System.out.println("OpenMetadata version: " + String.join(".", getClient().getClientVersion()));
    }
}
