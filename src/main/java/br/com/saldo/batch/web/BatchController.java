package br.com.saldo.batch.web;

import java.io.IOException;
import java.util.Map;

import br.com.saldo.batch.generator.AccountFileGenerator;
import br.com.saldo.batch.generator.AccountFileGenerator.GenerationResult;
import br.com.saldo.batch.model.RecordLayout;
import br.com.saldo.batch.service.BatchLauncherService;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BatchController {

    private final BatchLauncherService batchLauncher;
    private final AccountFileGenerator generator;

    public BatchController(BatchLauncherService batchLauncher,
                           AccountFileGenerator generator) {
        this.batchLauncher = batchLauncher;
        this.generator = generator;
    }

    @PostMapping("/batch/trigger-file")
    public ResponseEntity<Map<String, String>> triggerFile(
            @RequestParam(name = "file") String fileName) {
        batchLauncher.launchFileAsync(fileName);
        return ResponseEntity.accepted().body(Map.of(
                "status", "ACCEPTED",
                "job", "saldoFileJob",
                "file", fileName));
    }

    @PostMapping("/data/generate")
    public ResponseEntity<GenerationResult> generate(
            @RequestParam(name = "linesPerDigit", defaultValue = "100000") long linesPerDigit,
            @RequestParam(name = "date", defaultValue = "2026-07-07") String date,
            @RequestParam(name = "recordLength", defaultValue = "" + RecordLayout.RECORD_LENGTH) int recordLength,
            @RequestParam(name = "digit", required = false) Integer digit)
            throws IOException {
        return ResponseEntity.ok(generator.generate(linesPerDigit, date, recordLength, digit));
    }
}
