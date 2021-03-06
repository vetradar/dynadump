import * as AWS from 'aws-sdk';
import * as yargs from 'yargs';

import { ImportTable } from './import_table_locally';

const argv = yargs
  .usage('Dynadump\nUsage: $0 [options]')
  .help('help')
  .alias('help', 'h')
  .options({
    source: {
      alias: 's',
      description: `source table`,
      requiresArg: true,
      required: true,
    },
    destination: {
      alias: 'd',
      description: `destination table name`,
      requiresArg: true,
      required: false,
      default: '',
    },
    rowImportLimit: {
      alias: 'l',
      description: `limit the amount of rows imported`,
      requiresArg: true,
      required: false,
      default: 0,
    },
  }).argv;

const destinationTable = argv.destination || argv.source;

const importTable = new ImportTable({
  AWS,
  tableName: destinationTable,
  importTableName: argv.source,
  rowImportLimit: argv.rowImportLimit || 0,
});

importTable
  .process()
  .catch((err) => console.error(err))
  .finally(() => console.log('done'));
