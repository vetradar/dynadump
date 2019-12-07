import * as fs from 'fs-extra';

export type ExportTableOptions = {
  tableName: string;
  AWS: any;
  path: string;
};

export class ExportTable {
  _tableName: string;
  _dynamodb: any;
  _exportPath: string;

  constructor(options: ExportTableOptions) {
    this._tableName = options.tableName;
    this._dynamodb = new options.AWS.DynamoDB();
    this._exportPath = options.path || './export';
  }

  async process() {
    await this.exportTableDescription();
    return this.exportTable();
  }

  async exportTableDescription() {
    const tableDescription = await this._dynamodb
      .describeTable({
        TableName: this._tableName,
      })
      .promise();

    const formattedTableDescription = JSON.stringify(tableDescription, null, 2);

    await fs.outputFile(`${this._exportPath}/${this._tableName}.json`, formattedTableDescription, 'utf8');
  }

  async tableScan(lastEvaluatedKey) {
    const scanOptions = {
      TableName: this._tableName,
      ExclusiveStartKey: lastEvaluatedKey,
    };

    return this._dynamodb.scan(scanOptions).promise();
  }

  writeItems(stream: fs.WriteStream, items: [], lastEvaluatedKey: object) {
    items.forEach((item, index) => {
      stream.write('\r');
      stream.write(JSON.stringify(item));

      if ((lastEvaluatedKey === undefined && index < items.length - 1) || lastEvaluatedKey) {
        stream.write(`,`);
      }
    });
  }

  async exportTable() {
    const writeStream = fs.createWriteStream(`./export/${this._tableName}.data.json`);
    let totalItems = 0;

    try {
      let lastEvaluatedKey = null;
      writeStream.write('{ "data": [');

      do {
        const result = await this.tableScan(lastEvaluatedKey);
        lastEvaluatedKey = result.LastEvaluatedKey;
        this.writeItems(writeStream, result.Items, result.LastEvaluatedKey);
        totalItems = totalItems + result.Items.length;
      } while (lastEvaluatedKey);

      writeStream.write('\r],');
      writeStream.write(`\r"total": ${totalItems} }`);
    } catch (er) {
      console.error(er);
    } finally {
      writeStream.close();
    }
  }
}
