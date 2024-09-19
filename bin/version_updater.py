"""
Version Updater Script

This script automates the process of updating version numbers across multiple files
in a software project. It's designed to work with Databricks JDBC Driver project,
updating version strings in Java source files, test files, and the project's pom.xml.

Usage:
    Set the VERSION environment variable before running the script:
    export VERSION=x.y.z-qualifier
    python version_updater.py

The script performs the following tasks:
1. Validates the provided version number format.
2. Updates the version in the main DriverUtil.java file.
3. Updates the version in the project's pom.xml file.
4. Updates version assertions in multiple test files.

Requirements:
- Python 3.x
- Appropriate file permissions to read and write the target files

Note: This script assumes a specific project structure and file locations.
Adjust file paths if necessary.
"""

import os
import re
import sys


def _validate_version(version):
    pattern = r"^\d+\.\d+\.\d+-[a-zA-Z0-9]+$"
    return re.match(pattern, version) is not None


def _update_driver_util_java(file_path, new_version):
    with open(file_path, 'r') as file:
        content = file.read()

    updated_content = re.sub(
        r'private static final String VERSION = "[^"]*";',
        f'private static final String VERSION = "{new_version}";',
        content
    )

    with open(file_path, 'w') as file:
        file.write(updated_content)


def _update_assertions_test_file(file_path, new_version):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    # Update the version in the test assertion
    pattern = r'(assertTrue\(userAgent\.contains\("DatabricksJDBCDriverOSS/)[^"]+("\)\);)'
    try:
        updated_content = re.sub(pattern, r'\g<1>{}\g<2>'.format(new_version), content)

        if content == updated_content:
            print("Warning: Version in test file {} was not updated. Please check the file content.".format(file_path),
                  file=sys.stderr)
        else:
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(updated_content)
            print("Successfully updated version in test file: {}".format(file_path))
    except re.error as e:
        print("Error in regular expression: {}".format(str(e)), file=sys.stderr)
    except Exception as e:
        print("An error occurred while updating the test file: {}".format(str(e)), file=sys.stderr)


def _update_database_metadata_test_file(file_path, new_version):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    # Update the version in the metadata test assertion
    pattern = r'(assertEquals\(")[\d.]+-[a-zA-Z0-9]+("(?:, result)?\);)'
    updated_content = re.sub(pattern, r'\g<1>{}\g<2>'.format(new_version), content)

    if content == updated_content:
        print("Warning: Version in database metadata test file was not updated. Please check the file structure.",
              file=sys.stderr)

    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(updated_content)


def _update_pom_xml(file_path, new_version):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    # Update the version, allowing for comments between artifactId and version tags
    pattern = r'(<artifactId>databricks-jdbc</artifactId>\s*(?:<!--.*?-->\s*)?<version>)[^<]+(</version>)'
    updated_content = re.sub(pattern, r'\g<1>{}\g<2>'.format(new_version), content, count=1, flags=re.DOTALL)

    if content == updated_content:
        print("Warning: Version in pom.xml was not updated. Please check the file structure.", file=sys.stderr)

    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(updated_content)


def main():
    version = os.environ.get('VERSION')
    if not version:
        print("VERSION environment variable is not set.")
        return 1

    if not _validate_version(version):
        print(
            "Invalid version format. The version should be in the format: "
            "majorVersion.minorVersion.buildVersion-qualifier")
        return 1

    driver_util_file_path = "../src/main/java/com/databricks/jdbc/common/util/DriverUtil.java"
    pom_file_path = "../pom.xml"
    connection_test_file_path = "../src/test/java/com/databricks/jdbc/api/impl/DatabricksConnectionTest.java"
    database_metadata_test_file_path = ("../src/test/java/com/databricks/jdbc/api/impl/DatabricksDatabaseMetaDataTest"
                                        ".java")
    http_client_test_file_path = "../src/test/java/com/databricks/jdbc/dbclient/impl/http/DatabricksHttpClientTest.java"

    try:
        _update_driver_util_java(driver_util_file_path, version)
        print(f"Updated version in {driver_util_file_path}")

        _update_pom_xml(pom_file_path, version)
        print(f"Updated version in {pom_file_path}")

        _update_assertions_test_file(connection_test_file_path, version)
        print("Updated version in {}".format(connection_test_file_path))

        _update_database_metadata_test_file(database_metadata_test_file_path, version)
        print("Updated version in {}".format(database_metadata_test_file_path))

        _update_assertions_test_file(http_client_test_file_path, version)
        print("Updated version in {}".format(http_client_test_file_path))

        print(f"Version updated to {version}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
