#!/bin/bash

# Function to validate the version format
validate_version() {
  local version="$1"
  local pattern="^[0-9]+\.[0-9]+\.[0-9]+-[a-zA-Z0-9]+$"
  if [[ $version =~ $pattern ]]; then
    return 0
  else
    return 1
  fi
}

# Check if VERSION environment variable is set
if [ -z "$VERSION" ]; then
  echo "VERSION environment variable is not set."
  exit 1
fi

# Validate the version format
if ! validate_version "$VERSION"; then
  echo "Invalid version format. The version should be in the format: majorVersion.minorVersion.buildVersion-qualifier"
  exit 1
fi

# Determine the operating system
if [[ "$OSTYPE" == "darwin"* ]]; then
  # macOS
  SED_CMD="sed -i ''"
else
  # Linux/Ubuntu
  SED_CMD="sed -i"
fi

# Update version in DriverUtil.java
if ! eval "$SED_CMD 's|private static final String VERSION = \"[^\"]*\";|private static final String VERSION = \"$VERSION\";|' ../src/main/java/com/databricks/jdbc/commons/util/DriverUtil.java"; then
  echo "Failed to update version in DriverUtil.java"
  exit 1
fi

# Update version in pom.xml
if ! eval "$SED_CMD '/<artifactId>databricks-jdbc<\/artifactId>/,/<\/version>/s/<version>[^<]*<\/version>/<version>$VERSION<\/version>/' ../pom.xml"; then
  echo "Failed to update version in pom.xml"
  exit 1
fi

echo "Version updated to $VERSION"
