package br.com.saldo.batch.processor;

import br.com.saldo.batch.model.AccountRecord;
import br.com.saldo.batch.model.RecordLayout;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class LineProcessor implements ItemProcessor<String, AccountRecord> {

    @Override
    public AccountRecord process(String line) {
        if (line == null || line.length() < RecordLayout.MEANINGFUL_LENGTH) {
            return null;
        }
        String agency = line.substring(
                RecordLayout.AGENCY_OFFSET, RecordLayout.AGENCY_OFFSET + RecordLayout.AGENCY_LENGTH);
        String account = line.substring(
                RecordLayout.ACCOUNT_OFFSET, RecordLayout.ACCOUNT_OFFSET + RecordLayout.ACCOUNT_LENGTH);
        return new AccountRecord(agency + "-" + account, line);
    }
}
