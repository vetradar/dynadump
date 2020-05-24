import { ExportTable } from './export_table';

export type ExportDynamoDBOptions = {
  AWS: any;
  ignore: string[];
  path: string;
};

export class ExportDynamoDB {
  _AWS: any;
  _path: string;
  _toIgnore: string[];

  constructor(options: ExportDynamoDBOptions) {
    this._AWS = options.AWS;
    this._path = options.path || './export';
    this._toIgnore = options.ignore || [];
  }

  async process() {
    return this.exportAllTables();
  }

  async listTables(): Promise<string[]> {
    const dynamodb = new this._AWS.DynamoDB();
    const tables = await dynamodb.listTables().promise();

    console.log('Found tables:', tables.TableNames);
    console.log('Ignoring:', this._toIgnore);

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
    const tablesFiltered = tables.filter((tableName) => !this._toIgnore.includes(tableName));

    console.log('Exporting:', tablesFiltered);

    for (const table of tablesFiltered) {
      await this.exportTable(table);
      console.log(`Exported ${table}`);
    }
  }
}
