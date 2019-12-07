import { ExportTable } from './export_table';

export type ExportDynamoDBOptions = {
  AWS: any;
  path: string;
};

export class ExportDynamoDB {
  _AWS: any;
  _path: string;

  constructor(options: ExportDynamoDBOptions) {
    this._AWS = options.AWS;
    this._path = options.path || './export';
  }

  async process() {
    return this.exportAllTables();
  }

  async listTables() {
    const dynamodb = new this._AWS.DynamoDB();
    const tables = await dynamodb.listTables().promise();

    console.log(tables.TableNames);

    return tables.TableNames;
  }

  async exportTable(tableName: string) {
    const exportTable = new ExportTable({
      tableName,
      AWS: this._AWS,
      path: this._path,
    });

    return exportTable.process();
  }

  async exportAllTables() {
    const tables = await this.listTables();

    for (const table of tables) {
      await this.exportTable(table);
      console.log(`Exported ${table}`);
    }
  }
}
