import * as fs from 'fs-extra';
import { parser } from 'stream-json';
import { pick } from 'stream-json/filters/Pick';
import { streamArray } from 'stream-json/streamers/streamArray';
import { chain } from 'stream-chain';

export type ImportTableOptions = {
  importTableName: string;
  tableName: string;
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
  _dynamodb: any;

  constructor(options: ImportTableOptions) {
    this._importTableName = options.importTableName;
    this._tableName = options.tableName;

    this._dynamodb = new options.AWS.DynamoDB({
      endpoint: 'http://localhost:8000',
      region: 'us-west-2',
      accessKeyId: 'akey',
      secretAccessKey: 'asak',
    });
  }

  async process() {
    await this.readImportTableDescription();
    await this.dropTable();
    await this.createTable();

    return this.importData();
  }

  async readImportTableDescription() {
    this._importTableDescription = await fs.readJson(`./export/${this._importTableName}.json`);
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

    return this._dynamodb
      .deleteTable({
        TableName: this._tableName,
      })
      .promise();
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

    return this._dynamodb.createTable(newTableDescription).promise();
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
        console.log(error);
      });
  }

  async importData() {
    return new Promise((resolve, reject) => {
      const pipeline = chain([
        fs.createReadStream(`./export/${this._importTableName}.data.json`),
        parser(),
        pick({ filter: 'data' }),
        streamArray(),
        (data) => this.writeItemToDynamo(data),
      ]);

      pipeline.on('data', () => process.stdout.write('.'));

      pipeline.on('end', () => {
        resolve();
      });

      pipeline.on('error', (err) => {
        console.error(err);
        reject(err);
      });

      pipeline.on('finish', () => {
        console.log('finish');
        resolve();
      });
    });
  }
}
