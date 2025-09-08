package com.work.LogParser.Repositories;

import com.work.LogParser.Log;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LogRepository extends JpaRepository<Log, Long> {
}
