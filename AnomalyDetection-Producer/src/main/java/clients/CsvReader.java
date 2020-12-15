package clients;

import models.OpLog;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CsvReader {
    private final Pattern pattern;

    public CsvReader(final String separator) {
        this.pattern = Pattern.compile(separator);
    }

    public List<OpLog> loadCsvContentToList(
            final BufferedReader bufferedReader) throws IOException {
        try {
            return bufferedReader.lines().skip(1).map( line -> {
                final String[] lineArray = pattern.split(line);
                return new OpLog
                        .Builder()
                        .responseTime(lineArray[0])
                        .eventStartMs(lineArray[1])
                        .eventStartEpoc(lineArray[2])
                        .year(Integer.parseInt(lineArray[3]))
                        .month(Integer.parseInt(lineArray[4]))
                        .day(Integer.parseInt(lineArray[5]))
                        .build();
            }).collect(Collectors.toList());
        } finally {
            bufferedReader.close();
        }
    }
}