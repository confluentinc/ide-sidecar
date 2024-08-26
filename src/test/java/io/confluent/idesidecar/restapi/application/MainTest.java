package io.confluent.idesidecar.restapi.application;

import static io.confluent.idesidecar.restapi.application.Main.ExecutionOutcome.EXECUTING;
import static io.confluent.idesidecar.restapi.application.Main.ExecutionOutcome.UNMATCHED_ARGUMENTS;
import static io.confluent.idesidecar.restapi.application.Main.ExecutionOutcome.USAGE_INFO;
import static io.confluent.idesidecar.restapi.application.Main.ExecutionOutcome.VERSION_INFO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.confluent.idesidecar.restapi.application.Main.ExecutionOutcome;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import org.apache.maven.shared.utils.cli.CommandLineException;
import org.junit.jupiter.api.Test;

class MainTest {

  static final Pattern VERSION_PATTERN = Pattern.compile("^[0-9]+\\.[0-9]+\\.[0-9]+$");
  ByteArrayOutputStream output;

  @Test
  void shouldFindVersion() {
    // The quarkus.application.version config property is not set during IDE testing,
    // but will be set during `mvn test`
    if (!Main.VERSION.equals(Main.UNSET_VERSION)) {
      assertTrue(VERSION_PATTERN.matcher(Main.VERSION).matches());
    }
  }

  @Test
  void shouldIncludeVersionNumberInVersionInfo() {
    assertParseAndNotExecute(VERSION_INFO, "-v");
    var text  = output.toString();
    assertTrue(text.contains("ide-sidecar"));
    assertTrue(text.contains("version"));
    assertTrue(text.contains(Main.VERSION));
  }

  @Test
  void shouldExecuteAndDisplayVersionInfo() {
    assertParseAndNotExecute(VERSION_INFO, "-v");
    assertParseAndNotExecute(VERSION_INFO, "--version");
    // even when there are unknown arguments
    assertParseAndNotExecute(VERSION_INFO, "--version", "-x");
    assertParseAndNotExecute(VERSION_INFO, "--version", "--unknown");
    assertParseAndNotExecute(VERSION_INFO, "--version", "unknown");
  }

  @Test
  void shouldExecuteAndDisplayUsageInfo() {
    assertParseAndNotExecute(USAGE_INFO, "-h");
    assertParseAndNotExecute(USAGE_INFO, "--help");
    assertParseAndNotExecute(USAGE_INFO, "--help", "-v");
    // even when there are unknown arguments
    assertParseAndNotExecute(USAGE_INFO, "--help", "-x");
    assertParseAndNotExecute(USAGE_INFO, "--help", "--unknown");
    assertParseAndNotExecute(USAGE_INFO, "--help", "unknown");
  }

  @Test
  public void shouldNotRecognizeUnknownOptions() {
    assertParseAndNotExecute(UNMATCHED_ARGUMENTS, "-x");
    assertParseAndNotExecute(UNMATCHED_ARGUMENTS, "--unknown");
    assertParseAndNotExecute(UNMATCHED_ARGUMENTS, "unknown");
    assertParseAndNotExecute(UNMATCHED_ARGUMENTS, "@unknown");
    assertParseAndNotExecute(UNMATCHED_ARGUMENTS, "-xyz", "unknown");
    assertParseAndNotExecute(UNMATCHED_ARGUMENTS, "-x", "--unknown");
  }

  @Test
  public void shouldTreatOptionsWithIncorrectCaseAsUnknown() {
    assertParseAndNotExecute(UNMATCHED_ARGUMENTS, "-V");
    assertParseAndNotExecute(UNMATCHED_ARGUMENTS, "--VERSION");
    assertParseAndNotExecute(UNMATCHED_ARGUMENTS, "--vErSiOn");
  }

  @Test
  public void shouldExecuteWithNoArguments() {
    assertParseAndExecute(EXECUTING);
  }

  protected void assertParseAndNotExecute(ExecutionOutcome expected, String...args) {
    assertExecute(expected, false, args);
  }

  protected void assertParseAndExecute(ExecutionOutcome expected, String...args) {
    assertExecute(expected, true, args);
  }

  protected void assertExecute(ExecutionOutcome expected, boolean shouldRun, String...args) {
    output = new ByteArrayOutputStream();
    AtomicBoolean executed = new AtomicBoolean();
    try {
      var outcome = Main.execute(args, new PrintStream(output), () -> executed.set(true));
      assertEquals(expected, outcome);
      assertEquals(shouldRun, executed.get());
    } catch (CommandLineException e) {
      fail(e);
    }
  }
}