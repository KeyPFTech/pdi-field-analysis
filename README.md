# Field Analysis plugin

This plugin allows you to analyse any fields in the stream and outputs not just type information, but also mix/max and other mathematical analyses such as standard deviation, mean, mode, median, skewness, and dispersion.

The plugin is intended to be preceded by a select/values step that removes all but the fields you expect to be analysed from the stream.

The output is a single row per field, with columns describing that field.

Note that all rows are analysed, and so the speed of this plugin is directly related to N (the number of rows). 

Note also that this plugin blocks until all source rows are analysed - it does not stream.

## Building the plugin

You can use Microsoft Visual Studio Code (free) to build this, or manually.

### Visual Studio Code

1. Ensure you have Oracle Java 8 SDK (not JRE) installed on your computer
1. Ensure you have installed Maven globally on your computer
1. Open the pdi-field-analysis folder in Microsoft Code
1. Ensure you have the 'Language support for Java(tm) by RedHat' extension installed in Microsoft Visual Studio Code
1. Go to the Tasks menu, and choose Run Build Task...
1. Run the 'dist' build task

This will download all dependencies, and then build the project.

Note: On second and subsequent builds there is no need to download the dependencies again. To do this, run the dist-no-download task instead.

### Manual

Change to the root directory (the same as this readme file), and type the following:-

```sh
mvn install -Drelease -e
```

Note: After the first successful build you can tell Maven to not re-fetch build dependencies by using the o flag:-

```sh
mvn install -Drelease -e -o
```

## Installing the plugin

You now have a ZIP file in assemblies/plugin/target/ unpack this in to your PENTAHO_HOME/design-tools/data-integration/plugins folder.

WARNING: This repository builds to the latest version of PDI - likely a version you don't have installed! In order for PDI to not just ignore the plugin, after installing, edit the PENTAHO_HOME/design-tools/data-integration/plugins/field-analysis-plugin/version.xml file to list your version of PDI. E.g. 7.1.0.0-12. Save the file and restart PDI to use the plugin.
