package com.wp.redis.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "common.redisson")
public class RedissonConfigProperties {
    private String hostName;
    private int port;
    private String password;
    private int database;
    private int connectPoolSize;

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public int getConnectPoolSize() {
        return connectPoolSize;
    }

    public void setConnectPoolSize(int connectPoolSize) {
        this.connectPoolSize = connectPoolSize;
    }
}
