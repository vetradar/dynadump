# Dynadump
 
1. Dump dynamodb tables as JSON on local disk
2. Import exported data into **local dynamodb**

**NB: This does not support importing back into AWS Dynamodb instance**

# Why

Why another dynamodb export process? _I Couldn't find a working library,
either they modify the AWS Auth process, libraries outdated/doesn't
work, or way to complex with heaps of setup 😭_

# Prerequisites

```
npm i
npm run build
```

# Exporting

## Notes

Exports will create two files 

```
table_name.data.json
table_name.json
```

The data file contains the data, while the other contains the table
definition. The definition is used to create the table locally if it
doesn't exist.

**Beware if the table exists, it will replace the data.**

## Usage

There is no AWS Authentication input, this code will use standard Auth
cascades in the AWS SDK.

My preferred method is to use [AWS Vault](https://github.com/99designs/aws-vault)

```
# -p path to export (optional default is ./export)
> node lib/run_export_all [-p ./export]
```

# Importing

## Notes

Importing will try import to your local dynamodb instance, currently
hardcoded to `localhost:8080`, feel free to extend this.

```
# import a single table
# -s source table (required)
# -d destination table (optional)
> node lib/run_import_single [-s production_organisation] [-d dev_organisation] 
```

```
# import all tables
# -p path (default is ./export)
# -r replaces a matching string (can be regex) with -w (optional) 
# -w replacement string used with -r (required with -r)
> node lib/run_import_all [-p ./export] [-r production] [-w dev] 
```

# Contributing

PR's more than welcome ✌️
