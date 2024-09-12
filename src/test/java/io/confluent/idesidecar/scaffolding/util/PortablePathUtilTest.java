package io.confluent.idesidecar.scaffolding.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(Lifecycle.PER_CLASS)
class PortablePathUtilTest {
  Stream<Arguments> testPortablePath() {
    return Stream.of(
        Arguments.of("foo//bar/", "foo/bar/"),
        Arguments.of("foo/bar/", "foo/bar/"),
        Arguments.of("foo/bar\\baz", "foo/bar/baz"),
        Arguments.of("foo\\bar", "foo/bar"),
        Arguments.of("foo\\bar\\", "foo/bar/"),
        Arguments.of("foo/bar/", "foo/bar/"),
        Arguments.of("foo\\\\bar//", "foo/bar/")
    );
  }

  @ParameterizedTest
  @MethodSource
  void testPortablePath(String path1, String path2) {
    assertEquals(
        PortablePathUtil.portablePath(path1),
        PortablePathUtil.portablePath(path2),
        "Paths should be equal"
    );
  }

  Stream<Arguments> testPortablePathFragments() {
    return Stream.of(
        Arguments.of("foo/bar", new String[]{"foo", "bar"}),
        Arguments.of("foo/bar", new String[]{"foo", "bar/"}),
        Arguments.of("foo/bar", new String[]{"foo/", "bar"}),
        Arguments.of("foo/bar", new String[]{"foo/", "bar/"}),
        Arguments.of("foo/bar", new String[]{"foo/", "/bar/"}),
        Arguments.of("foo/bar", new String[]{"foo", "bar"}),
        Arguments.of("foo/bar", new String[]{"foo", "bar\\"}),
        Arguments.of("foo/bar", new String[]{"foo\\", "bar"}),
        Arguments.of("foo/bar", new String[]{"foo\\", "bar\\"}),
        Arguments.of("foo/bar", new String[]{"foo\\", "\\bar\\"})
    );
  }

  @ParameterizedTest
  @MethodSource
  void testPortablePathFragments(String expected, String... path2) {
    assertEquals(
        PortablePathUtil.portablePath(expected),
        PortablePathUtil.portablePath(path2),
        "Paths should be equal"
    );
  }
}