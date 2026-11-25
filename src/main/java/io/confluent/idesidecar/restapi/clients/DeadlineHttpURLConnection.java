package io.confluent.idesidecar.restapi.clients;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * An {@link HttpURLConnection} whose response body stops being read once a deadline passes. A
 * read timeout bounds only each read, so a server that sends its body a few bytes at a time could
 * otherwise keep a request going long past the deadline.
 *
 * <p>The check runs on the reading thread, before each read. Closing the connection from another
 * thread doesn't work here: closing the JDK's response stream waits for the lock that a blocked
 * read holds.
 */
final class DeadlineHttpURLConnection extends HttpURLConnection {

  private final HttpURLConnection delegate;
  private final Instant deadline;

  DeadlineHttpURLConnection(HttpURLConnection delegate, Instant deadline) {
    super(delegate.getURL());
    this.delegate = delegate;
    this.deadline = deadline;
  }

  /**
   * The error a request gets when it reaches its deadline.
   */
  static SocketTimeoutException deadlinePassed(String path) {
    return new SocketTimeoutException(
        "The deadline for Schema Registry request %s has passed".formatted(path)
    );
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return new DeadlineInputStream(delegate.getInputStream(), () -> deadlinePassed(url.getPath()));
  }

  /**
   * The error body, cut off at the deadline too. A cut-off or a read timeout throws
   * {@link ErrorBodyCutOff}, which keeps the status already received, so a slow 404 doesn't read as
   * a timeout.
   */
  @Override
  public InputStream getErrorStream() {
    var stream = delegate.getErrorStream();
    return stream == null ? null : new ErrorBodyInputStream(stream);
  }

  /**
   * The deadline passed while the body of an error response was still arriving.
   */
  static final class ErrorBodyCutOff extends SocketTimeoutException {

    private static final long serialVersionUID = 1L;

    private final int status;

    ErrorBodyCutOff(String path, int status) {
      super(
          "Schema Registry answered %d to request %s, but the rest didn't arrive in time"
              .formatted(status, path)
      );
      this.status = status;
    }

    /**
     * The response's HTTP status.
     */
    int status() {
      return status;
    }
  }

  /**
   * Builds the error a read throws once the deadline has passed.
   */
  @FunctionalInterface
  private interface DeadlineError {

    SocketTimeoutException create() throws IOException;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return delegate.getOutputStream();
  }

  @Override
  public void connect() throws IOException {
    delegate.connect();
  }

  @Override
  public void disconnect() {
    delegate.disconnect();
  }

  @Override
  public boolean usingProxy() {
    return delegate.usingProxy();
  }

  @Override
  public int getResponseCode() throws IOException {
    return delegate.getResponseCode();
  }

  @Override
  public String getResponseMessage() throws IOException {
    return delegate.getResponseMessage();
  }

  @Override
  public String getHeaderField(String name) {
    return delegate.getHeaderField(name);
  }

  @Override
  public String getHeaderField(int n) {
    return delegate.getHeaderField(n);
  }

  @Override
  public String getHeaderFieldKey(int n) {
    return delegate.getHeaderFieldKey(n);
  }

  @Override
  public Map<String, List<String>> getHeaderFields() {
    return delegate.getHeaderFields();
  }

  @Override
  public void setDoOutput(boolean doOutput) {
    delegate.setDoOutput(doOutput);
  }

  @Override
  public boolean getDoOutput() {
    return delegate.getDoOutput();
  }

  @Override
  public void setDoInput(boolean doInput) {
    delegate.setDoInput(doInput);
  }

  @Override
  public boolean getDoInput() {
    return delegate.getDoInput();
  }

  @Override
  public void setUseCaches(boolean useCaches) {
    delegate.setUseCaches(useCaches);
  }

  @Override
  public boolean getUseCaches() {
    return delegate.getUseCaches();
  }

  @Override
  public void setInstanceFollowRedirects(boolean followRedirects) {
    delegate.setInstanceFollowRedirects(followRedirects);
  }

  @Override
  public boolean getInstanceFollowRedirects() {
    return delegate.getInstanceFollowRedirects();
  }

  @Override
  public void setChunkedStreamingMode(int chunkLength) {
    delegate.setChunkedStreamingMode(chunkLength);
  }

  @Override
  public void setFixedLengthStreamingMode(int contentLength) {
    delegate.setFixedLengthStreamingMode(contentLength);
  }

  @Override
  public void setFixedLengthStreamingMode(long contentLength) {
    delegate.setFixedLengthStreamingMode(contentLength);
  }

  @Override
  public void addRequestProperty(String key, String value) {
    delegate.addRequestProperty(key, value);
  }

  @Override
  public Map<String, List<String>> getRequestProperties() {
    return delegate.getRequestProperties();
  }

  @Override
  public void setRequestMethod(String method) throws ProtocolException {
    delegate.setRequestMethod(method);
  }

  @Override
  public String getRequestMethod() {
    return delegate.getRequestMethod();
  }

  @Override
  public void setRequestProperty(String key, String value) {
    delegate.setRequestProperty(key, value);
  }

  @Override
  public String getRequestProperty(String key) {
    return delegate.getRequestProperty(key);
  }

  @Override
  public void setConnectTimeout(int timeout) {
    delegate.setConnectTimeout(timeout);
  }

  @Override
  public int getConnectTimeout() {
    return delegate.getConnectTimeout();
  }

  @Override
  public void setReadTimeout(int timeout) {
    delegate.setReadTimeout(timeout);
  }

  @Override
  public int getReadTimeout() {
    return delegate.getReadTimeout();
  }

  private class DeadlineInputStream extends FilterInputStream {

    private final DeadlineError error;

    DeadlineInputStream(InputStream in, DeadlineError error) {
      super(in);
      this.error = error;
    }

    @Override
    public int read() throws IOException {
      checkDeadline();
      return super.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      checkDeadline();
      return super.read(b, off, len);
    }

    private void checkDeadline() throws IOException {
      if (!Instant.now().isBefore(deadline)) {
        throw error.create();
      }
    }
  }

  /**
   * An error body, whose timeouts keep the response's status.
   */
  private final class ErrorBodyInputStream extends DeadlineInputStream {

    private ErrorBodyInputStream(InputStream in) {
      super(in, () -> new ErrorBodyCutOff(url.getPath(), delegate.getResponseCode()));
    }

    @Override
    public int read() throws IOException {
      try {
        return super.read();
      } catch (SocketTimeoutException e) {
        throw keepingStatus(e);
      }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      try {
        return super.read(b, off, len);
      } catch (SocketTimeoutException e) {
        throw keepingStatus(e);
      }
    }

    private ErrorBodyCutOff keepingStatus(SocketTimeoutException timeout) throws IOException {
      if (timeout instanceof ErrorBodyCutOff cutOff) {
        return cutOff;
      }
      var cutOff = new ErrorBodyCutOff(url.getPath(), delegate.getResponseCode());
      cutOff.initCause(timeout);
      return cutOff;
    }
  }
}
