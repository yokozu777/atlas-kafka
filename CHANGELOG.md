# Changelog

## [Unreleased]

### Added
- `200_kafka_node`: Oracle Linux support — install `java-17-openjdk-headless`
  (+ tar, ca-certificates) via dnf. Enables mixed-OS `ci/kafka` (Phase 2/3).
- `kafka_user_shell` default (`/usr/sbin/nologin`) for portable user creation.
- Unsupported-OS fail message includes `os_family` + `distribution_version`.
- Separate apt Java packages: Ubuntu `openjdk-17-jre-headless`, Debian
  `openjdk-21-jre-headless` (Debian 13 / trixie mirror has no OpenJDK 17).

### Fixed
- Docs: Compatibility lists Ubuntu/Debian/OracleLinux consistently with the role.
- Debian 13 Java install: no longer share Ubuntu's OpenJDK 17 package name.
