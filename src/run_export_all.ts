import * as AWS from 'aws-sdk';
import * as yargs from 'yargs';

import { ExportDynamoDB } from './export_all_tables';

const argv = yargs
  .usage('Dynadump\nUsage: $0 [options]')
  .help('help')
  .alias('help', 'h')
  .options({
    path: {
      alias: 'p',
      description: 'data export path',
      requiresArg: true,
      required: false,
      default: './export',
    },
    ignore: {
      type: 'array',
      alias: 'i',
      description: 'table names to skip export of',
      requiresArg: true,
      required: false,
      default: '[]',
    },
  })
  .implies('with', 'replace').argv;

const exportDynamoDB = new ExportDynamoDB({
  AWS,
  path: argv.path,
  ignore: argv.ignore,
});

exportDynamoDB.process().finally(() => console.log('done'));
