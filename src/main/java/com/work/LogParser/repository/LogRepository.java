package com.work.LogParser.repository;

import com.work.LogParser.entity.LogEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface LogRepository extends JpaRepository<LogEntry, Long> {

    @Modifying
    @Transactional
    @Query(value = "TRUNCATE TABLE logs", nativeQuery = true)
    void truncateTable();

    @Query(value = "SELECT COUNT(*) FROM logs", nativeQuery = true)
    long countAll();
}