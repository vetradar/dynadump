import * as fs from 'fs-extra';
import { parser } from 'stream-json';
import { pick } from 'stream-json/filters/Pick';
import { streamArray } from 'stream-json/streamers/streamArray';
import { chain } from 'stream-chain';
import * as readline from 'readline';
import * as chalk from 'chalk';

export type ImportTableOptions = {
  importTableName: string;
  tableName: string;
  rowImportLimit: number;
  AWS: any;
};

type TableIndex = {
  IndexName: string;
  KeySchema: [];
  Projection: {
    ProjectionType: string;
  };
  ProvisionedThroughput: {
    NumberOfDecreasesToday: number;
    ReadCapacityUnits: number;
    WriteCapacityUnits: number;
  };
};

type TableDescription = {
  Table: {
    AttributeDefinitions: [];
    TableName: string;
    KeySchema: [];
    ProvisionedThroughput: {
      NumberOfDecreasesToday: number;
      ReadCapacityUnits: number;
      WriteCapacityUnits: number;
    };
    GlobalSecondaryIndexes?: TableIndex[];
    LocalSecondaryIndexes?: TableIndex[];
  };
};

export class ImportTable {
  _importTableName: string;
  _importTableDescription: TableDescription;
  _tableName: string;
  _rowImportLimit: number;
  _dynamodb: any;

  constructor(options: ImportTableOptions) {
    this._importTableName = options.importTableName;
    this._tableName = options.tableName;
    this._rowImportLimit = options.rowImportLimit;

    this._dynamodb = new options.AWS.DynamoDB({
      endpoint: 'http://localhost:8000',
      region: 'us-west-2',
      accessKeyId: 'akey',
      secretAccessKey: 'asak',
    });
  }

  async process() {
    console.log(chalk.blue.bgYellow.bold(`Starting import on ${this._importTableName}`));
    console.log(chalk(`Destination table name: ${chalk.green(this._tableName)}`));
    console.log(chalk(`Row import limit: ${chalk.green(this._rowImportLimit)}`));

    await this.readImportTableDescription();
    await this.dropTable();
    await this.createTable();

    try {
      await this.importData();
    } catch (error) {
      console.log(error);
    }

    console.log(chalk.blue.bgGreen.bold(`Completed import on ${this._importTableName}`));
  }

  async readImportTableDescription() {
    this._importTableDescription = await fs.readJson(`./export/${this._importTableName}.json`);

    console.log(chalk(`Table ${chalk.green(this._tableName)} description read.`));
  }

  async tableExists() {
    const tables = await this._dynamodb.listTables().promise();
    return tables.TableNames.includes(this._tableName);
  }

  async dropTable() {
    const tableExists = await this.tableExists();

    if (!tableExists) {
      return null;
    }

    await this._dynamodb
      .deleteTable({
        TableName: this._tableName,
      })
      .promise();

    console.log(chalk(`Table ${chalk.red(this._tableName)} dropped.`));
  }

  async createTable() {
    const tableExists = await this.tableExists();

    if (tableExists) {
      return null;
    }

    const newTableDescription = {
      TableName: this._tableName,
      AttributeDefinitions: this._importTableDescription.Table.AttributeDefinitions,
      KeySchema: this._importTableDescription.Table.KeySchema,
      ProvisionedThroughput: {
        ReadCapacityUnits: this._importTableDescription.Table.ProvisionedThroughput.ReadCapacityUnits || 1,
        WriteCapacityUnits: this._importTableDescription.Table.ProvisionedThroughput.WriteCapacityUnits || 1,
      },
    };

    if (this._importTableDescription.Table.GlobalSecondaryIndexes) {
      // @ts-ignore
      newTableDescription.GlobalSecondaryIndexes = [];
    }

    this._importTableDescription.Table.GlobalSecondaryIndexes?.forEach((gsi) => {
      // @ts-ignore
      newTableDescription.GlobalSecondaryIndexes.push({
        IndexName: gsi.IndexName,
        KeySchema: [...gsi.KeySchema],
        Projection: gsi.Projection,
        ProvisionedThroughput: {
          ReadCapacityUnits: gsi.ProvisionedThroughput.ReadCapacityUnits || 1,
          WriteCapacityUnits: gsi.ProvisionedThroughput.WriteCapacityUnits || 1,
        },
      });
    });

    if (this._importTableDescription.Table.LocalSecondaryIndexes) {
      // @ts-ignore
      newTableDescription.LocalSecondaryIndexes = [];
    }

    this._importTableDescription.Table.LocalSecondaryIndexes?.forEach((lsi) => {
      // @ts-ignore
      newTableDescription.LocalSecondaryIndexes.push({
        IndexName: lsi.IndexName,
        KeySchema: [...lsi.KeySchema],
        Projection: lsi.Projection,
      });
    });

    await this._dynamodb.createTable(newTableDescription).promise();

    console.log(chalk(`Table ${chalk.green(this._tableName)} created.`));
  }

  async writeItemToDynamo(item) {
    const params = {
      Item: item.value,
      TableName: this._tableName,
    };

    return this._dynamodb
      .putItem(params)
      .promise()
      .catch((error) => {
        console.log(chalk(`${chalk.red(error)}`));
      });
  }

  writeRowsWritten(rows) {
    readline.clearLine(process.stdout, 0);
    readline.cursorTo(process.stdout, 0, null);
    let text = `items written: ${rows} \r`;
    process.stdout.write(text);
  }

  async importData() {
    let itemsWrittenToDynamo = 0;
    let streamClosed = false;

    return new Promise((resolve, reject) => {
      const fileStream = fs.createReadStream(`./export/${this._importTableName}.data.json`);

      const pipeline = chain([
        fileStream,
        parser(),
        pick({ filter: 'data' }),
        streamArray(),
        (data) => {
          if (!streamClosed) {
            return this.writeItemToDynamo(data);
          }

          return Promise.resolve();
        },
      ]);

      pipeline.on('data', () => {
        itemsWrittenToDynamo = itemsWrittenToDynamo + 1;

        this.writeRowsWritten(itemsWrittenToDynamo);

        if (this._rowImportLimit !== 0 && itemsWrittenToDynamo >= this._rowImportLimit) {
          pipeline.destroy();
          console.log('');
          streamClosed = true;
        }
      });

      pipeline.on('end', () => {
        resolve();
      });

      pipeline.on('error', (err) => {
        console.error(err);
        reject(err);
      });

      pipeline.on('close', () => {
        resolve();
      });

      pipeline.on('finish', () => {
        resolve();
      });
    }).catch((error) => {
      console.log(chalk(`${chalk.red(error)}`));
    });
  }
}
