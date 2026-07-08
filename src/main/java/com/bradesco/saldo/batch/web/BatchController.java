package com.bradesco.saldo.batch.web;

import java.io.IOException;
import java.util.Map;

import com.bradesco.saldo.batch.generator.AccountFileGenerator;
import com.bradesco.saldo.batch.generator.AccountFileGenerator.GenerationResult;
import com.bradesco.saldo.batch.model.RecordLayout;
import com.bradesco.saldo.batch.service.BatchLauncherService;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BatchController {

    private final BatchLauncherService batchLauncher;
    private final AccountFileGenerator generator;
    private final String defaultInputDir;

    public BatchController(BatchLauncherService batchLauncher,
                           AccountFileGenerator generator,
                           @Value("${app.input-dir}") String defaultInputDir) {
        this.batchLauncher = batchLauncher;
        this.generator = generator;
        this.defaultInputDir = defaultInputDir;
    }

    @PostMapping("/batch/trigger")
    public ResponseEntity<Map<String, String>> trigger(
            @RequestParam(name = "run", required = false) String run) {
        String runId = (run == null || run.isBlank()) ? "run-" + System.currentTimeMillis() : run;
        batchLauncher.launchAsync(runId);
        return ResponseEntity.accepted().body(Map.of(
                "status", "ACCEPTED",
                "job", "saldoBatchJob",
                "run", runId));
    }

    @PostMapping("/data/generate")
    public ResponseEntity<GenerationResult> generate(
            @RequestParam(name = "linesPerDigit", defaultValue = "100000") long linesPerDigit,
            @RequestParam(name = "dir", required = false) String dir,
            @RequestParam(name = "date", defaultValue = "2026-07-07") String date,
            @RequestParam(name = "recordLength", defaultValue = "" + RecordLayout.RECORD_LENGTH) int recordLength)
            throws IOException {
        String targetDir = (dir == null || dir.isBlank()) ? defaultInputDir : dir;
        return ResponseEntity.ok(generator.generate(linesPerDigit, targetDir, date, recordLength));
    }
}
