package io.confluent.idesidecar.restapi.application;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;
import java.io.PrintStream;
import org.apache.maven.shared.utils.cli.CommandLineException;
import org.apache.maven.shared.utils.cli.CommandLineUtils;
import org.eclipse.microprofile.config.ConfigProvider;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Option;

/**
 * The main application that is run by Quarkus, with the ability to parse and use command line
 * arguments. For example, this allows a user to run the sidecar ask for usage (with {@code -h} or
 * {@code --help}) or version information (with {@code -v} or {@code --version}).
 * Options are case sensitive.
 */
@QuarkusMain
public class Main {


  /**
   * The command line options for this application.
   */
  @Command(name = APPLICATION_NAME, versionProvider = ManifestVersionProvider.class)
  static class Options {

    @Option(
        names = {"-v", "--version"},
        versionHelp = true,
        description = "display the version"
    )
    boolean versionInfoRequested;

    @Option(
        names = {"-h", "--help"},
        usageHelp = true,
        description = "display this help message"
    )
    boolean usageHelpRequested;
  }

  /**
   * The name of the application used in usage and version information.
   */
  public static final String APPLICATION_NAME = "ide-sidecar";

  static final int ERROR_CODE_FOR_ERROR_PARSING_ARGUMENTS = 2;

  /**
   * The default value for the version, used primarily with some testing environments
   */
  static final String UNSET_VERSION = "not-set";

  /**
   * The version of the application. Unfortunately, this is not set during unit tests.
   */
  static final String VERSION = ConfigProvider
      .getConfig()
      .getOptionalValue("quarkus.application.version", String.class)
      .orElse(UNSET_VERSION);

  /**
   * The {@link #execute(String[], PrintStream, Runnable)} encapsulates most of the
   * command line processing logic and is easily testable.
   */
  public static void main(String... args) {
    try {
      execute(args, System.out, () -> Quarkus.run(args));
    } catch (CommandLineException e) {
      System.err.println("Error processing command line: " + e.getMessage());
      System.exit(ERROR_CODE_FOR_ERROR_PARSING_ARGUMENTS);
    }
  }

  /**
   * Parse the supplied arguments, and run the supplied executable if applicable.
   * Some arguments, such as getting the application version or usage help, will cause
   * the application to exit quickly rather than running the executable.
   *
   * <p>This method exists to encapsulate most of the command line processing logic and is
   * designed to be easily testable. The {@link ExecutionOutcome} type is used by this method
   * to return the outcome of this logic, so that tests can verify the inputs resulted in the
   * expected output.
   *
   * @param args       the options and parameters supplied on the command line
   * @param output     the print stream to use for output
   * @param executable the application function to run if the arguments are valid and do not
   *                   call for exiting immediately
   * @return the outcome
   * @throws CommandLineException if there is an error parsing the command line
   */
  static ExecutionOutcome execute(
      String[] args,
      PrintStream output,
      Runnable executable
  ) throws CommandLineException {
    if (args.length != 0) {
      // There is at least one command line argument
      if (args.length == 1) {
        // Maven will submit all args as a single string, so maybe break them up
        args = CommandLineUtils.translateCommandline(args[0]);
      }

      // Parse the command line
      Options options = new Options();
      CommandLine commandLine = new CommandLine(options)
          .setStopAtUnmatched(true)
          .setOptionsCaseInsensitive(false)
          .setAbbreviatedOptionsAllowed(true)
          .setUsageHelpAutoWidth(true);
      commandLine.parseArgs(args);
      if (options.usageHelpRequested) {
        commandLine.usage(output);
        return ExecutionOutcome.USAGE_INFO;
      } else if (options.versionInfoRequested) {
        commandLine.printVersionHelp(output);
        return ExecutionOutcome.VERSION_INFO;
      } else if (!commandLine.getUnmatchedArguments().isEmpty()) {
        return ExecutionOutcome.UNMATCHED_ARGUMENTS;
      }
    }

    // Otherwise run the application
    if (executable != null) {
      executable.run();
    }
    return ExecutionOutcome.EXECUTING;
  }

  enum ExecutionOutcome {
    EXECUTING,
    VERSION_INFO,
    USAGE_INFO,
    UNMATCHED_ARGUMENTS,
  }

  /**
   * Get the version of the application.
   */
  static class ManifestVersionProvider implements IVersionProvider {
    public String[] getVersion() {
      return new String[] { "${COMMAND-FULL-NAME} version %s".formatted(VERSION)};
    }
  }

}
