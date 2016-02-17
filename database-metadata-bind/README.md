database-metadata-bind
====================
[![Build Status](https://travis-ci.org/jinahya/database-metadata-bind.svg?branch=develop)](https://travis-ci.org/jinahya/database-metadata-bind)
[![Dependency Status](https://www.versioneye.com/user/projects/563ccf434d415e0018000001/badge.svg)](https://www.versioneye.com/user/projects/563ccf434d415e0018000001)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.jinahya/database-metadata-bind.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.github.jinahya%22%20a%3A%22database-metadata-bind%22)
[![Support via Gratipay](https://img.shields.io/gratipay/JSFiddle.svg)](https://gratipay.com/~jinahya/)
[![Codacy Badge](https://api.codacy.com/project/badge/grade/2e056714e9614bf89b860601cbb2b174)](https://www.codacy.com/app/jinahya/database-metadata-bind)
[![Domate via Paypal](https://img.shields.io/badge/donate-paypal-blue.svg)](https://www.paypal.com/cgi-bin/webscr?cmd=_cart&business=A954LDFBW4B9N&lc=KR&item_name=GitHub&amount=5%2e00&currency_code=USD&button_subtype=products&add=1&bn=PP%2dShopCartBF%3adonate%2dpaypal%2dblue%2epng%3aNonHosted)

A library binding various information from [DatabaseMetaData](http://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html).

## Versions

| Version        | Apidocs | Site | Notes |
| :------        | :------ | :--- | :---- |
| `1.0` | [apidocs](http://jinahya.github.io/database-metadata-bind/site/1.0/apidocs/index.html) | [site](http://jinahya.github.io/database-metadata-bind/site/1.0/index.html)|Not Released Yet|
| `0.17` | [apidocs](http://jinahya.github.io/database-metadata-bind/site/0.17/apidocs/index.html) | [site](http://jinahya.github.io/database-metadata-bind/site/0.17/index.html)||
| `0.17-SNAPSHOT` | [apidocs](http://jinahya.github.io/database-metadata-bind/site/0.17-SNAPSHOT/apidocs/index.html) | [site](http://jinahya.github.io/database-metadata-bind/site/0.17-SNAPSHOT/index.html)||
| `0.16` | [apidocs](http://jinahya.github.io/database-metadata-bind/site/0.16/apidocs/index.html) | [site](http://jinahya.github.io/database-metadata-bind/site/0.16/index.html)||

## Hierarchy
![Class Diagram](http://jinahya.github.io/database-metadata-bind/site/0.16/apidocs/com/github/jinahya/sql/database/metadata/bind/com.github.jinahya.sql.database.metadata.bind.png)
<!--
  * Metadata
    * Category (`metadata/categories`)
      * Schema (`category/schemas`)
        * Function (`schema/functions`)
          * FunctionColumn (`function/functionColumns`)
        * Procedure (`schema/procedures`)
          * ProcedureColumn (`procedure/procedureColuns`)
        * Table (`schema/tables`)
          * BestRowIdentifier (`table/bestRowIdentifiers`)
          * Column (`table/columns`)
            * ColumnPrivilege (`column/columnPrivileges`)
          * ExportedKey (`table/exportedKeys`)
          * ImportedKey (`table/importedKeys`)
          * IndexInfo (`table/IndexInfo`)
          * PrimaryKey (`table/primaryKeys`)
          * PseudoColumn (`table/pseudoColumns`)
          * TablePrivilege (`table/tablePrivileges`)
          * VersionColumn (`table/versionColumns`)
        * UserDefinedType (`schema/userDefinedTypes`)
    * ClientInfoProperty (`metadata/clientInfoProperties`)
    * SchemaName (`metadata/schemaNames`)
    * TableType (`metadata/tableTypes`)
-->

## Usage
### API Binding
````java
// prepare jdbc information
final Connection connection; // get your own
final DatabaseMetaData database = connection.getDataBaseMetaData();

// create context, and add suppressions if required
final MetadataContext context = new MetaDataContext(database);
context.addSuppressions("metadata/schemaNames", "table/pseudoColumns");

// bind various informations
final Metadata metadata = context.getMetadata(); // bind all
final List<Categories> categories = metadata.getCategories();
for (final Category category : categories) {
    final List<Schema> schemas = category.getSchemas();
}
final List<Schema> schemas = context.getSchemas("", null);
final List<Tables> tables = context.getTables(null, null, null); // list all tables
final List<PrimaryKeys> primaryKeys
    = context.getPrimaryKeys("PUBLIC", "SYSTEM_LOBS", "BLOCKS");
````
### XML Binding
All classes are annotated with `@XmlRootElement`.
````java
final UseDefinedType userDefinedType;
final JAXBContext context = JAXBContext.newInstance(UserDefinedType.class);
final Marshaller marshaller = context.createMarshaller();
marshaller.mashal(userDefinedType, ...);
````
----
[![Domate via Paypal](https://img.shields.io/badge/donate-paypal-blue.svg)](https://www.paypal.com/cgi-bin/webscr?cmd=_cart&business=A954LDFBW4B9N&lc=KR&item_name=GitHub&amount=5%2e00&currency_code=USD&button_subtype=products&add=1&bn=PP%2dShopCartBF%3adonate%2dpaypal%2dblue%2epng%3aNonHosted)

