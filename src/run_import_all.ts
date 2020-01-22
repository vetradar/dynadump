import * as fs from 'fs-extra';
import * as AWS from 'aws-sdk';
import * as yargs from 'yargs';

import { ImportTable } from './import_table_locally';

const findExportedTables = (opts): string[] => {
  const dataFileFormat = /([a-zA-Z0-9._-]*)(\.data)\.json/;

  const files = fs.readdirSync(opts.path);

  const validTables = [];

  for (const file of files) {
    const match = file.match(dataFileFormat);

    // test that we have an accompanying table definition
    if (match && files.includes(`${match[1]}.json`)) {
      validTables.push(match[1]);
    }
  }

  return validTables;
};

const importAll = async (opts) => {
  console.log(opts);

  const tables = findExportedTables(opts);

  for (const table of tables) {
    const replaceString = opts.replace.replace(/\\(.)/g, '$1');
    const withString = opts.with.replace(/\\(.)/g, '$1');

    const importTable = new ImportTable({
      AWS,
      tableName: table.replace(replaceString, withString),
      importTableName: table,
      rowImportLimit: opts.rowImportLimit,
    });

    await importTable.process();
  }
};

const argv = yargs
  .usage('Dynadump\nUsage: $0 [options]')
  .help('help')
  .alias('help', 'h')
  .options({
    replace: {
      alias: 'r',
      description: `replaces matching string in table. Example -r \\bleh\/ can be regex`,
      requiresArg: true,
      required: false,
      default: '',
    },
    with: {
      alias: 'w',
      description: `replacement string`,
      requiresArg: true,
      required: false,
      default: '',
    },
    path: {
      alias: 'p',
      description: 'import folder path',
      requiresArg: true,
      required: false,
      default: './export',
    },
    rowImportLimit: {
      alias: 'l',
      description: `limit the amount of rows imported`,
      requiresArg: true,
      required: false,
      default: 0,
    },
  })
  .implies('with', 'replace').argv;

importAll(argv).finally(() => console.log('done'));
