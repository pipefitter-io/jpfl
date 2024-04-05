package io.pipefitter.jpfl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

class PipeEndToEndTest {
    final Path inputPath = Paths.get("src/test/resources/fixtures/input.txt");
    final Path outputPath = Paths.get("src/test/resources/fixtures/output.json");

    @Test
    void testProcessesTextFileThroughStagesAndProducesFilteredJsonOutput() throws IOException {
        Pipe pipe = new Pipe();
        pipe.addStage(new FileReader(inputPath.toString()));
        pipe.addStage(new RetractionFilter());
        pipe.addStage(new WidgetTxtToJson());
        pipe.addStage(new FileWriter(outputPath.toString()));

        pipe.execute();

        assertTrue(Files.exists(outputPath), "Output file does not exist");
        String expectedContent = "{\"content\":\"This is a test.\\n\"}";
        assertEquals(expectedContent, Files.readString(outputPath), "File content does not match expected content");
    }

    // Embedded Stage Implementations

    private static class FileReader extends Source {
        private final Path filepath;

        public FileReader(String filepath) {
            this.filepath = Path.of(filepath);
        }

        @Override
        protected Message produce(Message message) {
            try {
                String content = Files.readString(filepath);
                return new Message("txt_file", content);
            } catch (IOException e) {
                e.printStackTrace();
                return null; // Or handle more gracefully
            }
        }
    }

    private static class RetractionFilter extends Filter {
        @Override
        protected Message filterCriteria(Message message) {
            String retractedPayload = message.getPayload().replace("Unneeded data", "");
            return new Message(message.getProtocol(), retractedPayload);
        }
    }

    private static class WidgetTxtToJson extends Transformer {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        protected Message transformItem(Message message) {
            try {
                Map<String, Object> contentMap = new HashMap<>();
                contentMap.put("content", message.getPayload());
                String jsonContent = objectMapper.writeValueAsString(contentMap);
                return new Message("json", jsonContent);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static class FileWriter extends Sink {
        private final Path filepath;

        public FileWriter(String filepath) {
            this.filepath = Path.of(filepath);
        }

        @Override
        protected Message consumeItem(Message message) {
            try {
                Files.writeString(filepath, message.getPayload());
                System.out.println("File written successfully to " + filepath);
                return message;
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException("Failed to write file: " + filepath, e);
            }
        }
    }
}
