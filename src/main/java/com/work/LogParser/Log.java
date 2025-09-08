package com.work.LogParser;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "logs")
public class Log {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "time")
    private LocalDateTime time;

    @Column(name = "ip")
    private String ip;

    @Column(name = "url")
    private String url;

    @Column(name = "username")
    private String username;

    // Конструкторы, геттеры и сеттеры
    public Log() {}

    public Log(LocalDateTime time, String ip, String url, String username) {
        this.time = time;
        this.ip = ip;
        this.url = url;
        this.username = username;
    }

    public Long getId() { return id; }
    public LocalDateTime getTime() { return time; }
    public void setTime(LocalDateTime time) { this.time = time; }
    public String getIp() { return ip; }
    public void setIp(String ip) { this.ip = ip; }
    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }
    public String getUser() { return username; }
    public void setUser(String username) { this.username = username; }
}

