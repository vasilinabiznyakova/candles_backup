const ClickHouse = require('@apla/clickhouse');
const axios = require('axios');
const dotenv = require('dotenv');
const fs = require('fs');
const path = require('path');
const retry = require('retry');
const csvParser = require('csv-parser');
const stringify = require('csv-stringify');

dotenv.config();

const clickhouse = new ClickHouse({
  host: process.env.CH_HOST,
  port: process.env.CH_PORT,
  user: process.env.CH_USER,
  password: process.env.CH_PASSWORD,
  timeout: 60000,
  max_open_connections: 20,
});

const missingIntervalsDir = path.join(__dirname, 'missing_intervals');
const market = process.env.MARKET.toLowerCase();

async function initCandlesBackup() {
  console.log('initCandlesBackup started working');
  // await uploadCsvFilesToDB();
  // console.log('Script finished');

  if (!fs.existsSync(missingIntervalsDir)) {
    fs.mkdirSync(missingIntervalsDir);
    console.log(`Directory created: ${missingIntervalsDir}`);
  } else {
    console.log(`Directory exists: ${missingIntervalsDir}`);
  }
  await getMissingIntervalsFiles();
  await readFilesFromDirectory(missingIntervalsDir);
  await removeDuplicates();
}

function executeQuery(query) {
  return new Promise((resolve, reject) => {
    const operation = retry.operation({
      retries: 5,
      factor: 2,
      minTimeout: 1000,
      maxTimeout: 60000,
      randomize: true,
    });

    operation.attempt(() => {
      const rows = [];
      const stream = clickhouse.query(query);

      stream.on('data', (row) => rows.push(row));
      stream.on('end', () => resolve(rows));
      stream.on('error', (err) => {
        if (operation.retry(err)) {
          return;
        }
        reject(operation.mainError());
      });
    });
  });
}

async function getMissingIntervalsFiles() {
  try {
    let tables;
    if (market === 'binance_futures') {
      const tablesQuery = `
        SELECT name
        FROM system.tables
        WHERE database = 'db_candles_${market}'
      `;
      tables = await executeQuery(tablesQuery);
    } else {
      const assetsFilePath = path.join(__dirname, 'assets.json');
      // Wrapping fs.readFile in a Promise for async/await usage
      const fileContent = await new Promise((resolve, reject) => {
        fs.readFile(assetsFilePath, 'utf8', (err, data) => {
          if (err) {
            return reject(err);
          }
          resolve(data);
        });
      });
      tables = JSON.parse(fileContent);
    }

    console.log(tables);

    if (!fs.existsSync(missingIntervalsDir)) {
      fs.mkdirSync(missingIntervalsDir);
    }

    for (const table of tables) {
      let tableName;
      if (market === 'binance_futures') {
        tableName = table[0];
      } else {
        tableName = `tbl_${table}`;
      }

      const query = `
          SELECT ts_start
          FROM db_candles_${market}.${tableName}
          WHERE ts_start >= toUnixTimestamp(toStartOfInterval(now() - INTERVAL 2 YEAR, INTERVAL 1 SECOND)) * 1000
          ORDER BY ts_start ASC
        `;

      const rows = await executeQuery(query);
      if (rows.length > 0) {
        const missingIntervals = findMissingIntervals(rows);
        if (missingIntervals.length > 0) {
          const result = { [tableName]: missingIntervals };
          const filePath = path.join(
            missingIntervalsDir,
            `missing_intervals_${tableName}.json`
          );
          fs.writeFileSync(filePath, JSON.stringify(result, null, 2));
          console.log(
            `Missing intervals for ${tableName} have been saved to ${filePath}`
          );
          console.log(
            `Missing intervals for ${tableName} have been saved to ${filePath}`
          );
        }
      } else {
        console.log(`No missing intervals for ${tableName}`);
      }
    }
  } catch (err) {
    console.error('Error:', err);
  }
}

function findMissingIntervals(data) {
  const missingIntervals = [];

  for (let i = 1; i < data.length; i++) {
    const prevTimestamp = Number(data[i - 1]);
    const currentTimestamp = Number(data[i]);
    const timeDiff = currentTimestamp - prevTimestamp;
    if (timeDiff > 60000) {
      const startInterval = prevTimestamp + 60000;
      const finishInterval = currentTimestamp;
      missingIntervals.push({
        start: startInterval,
        end: finishInterval,
      });
    }
  }
  return missingIntervals;
}

const interval = '1m';
const BinanceWeightLimit = 40;
const batchSize = 1500;

async function readFilesFromDirectory(directory) {
  console.log(`Reading files from directory: ${directory}`);

  let currentSymbol = null;
  let batch = [];
  let leftReq = BinanceWeightLimit;

  try {
    const files = fs.readdirSync(directory);
    console.log(`Found ${files.length} files.`);

    for (const file of files) {
      const filePath = path.join(directory, file);
      try {
        const data = fs.readFileSync(filePath, 'utf8');
        const jsonData = JSON.parse(data);
        const tableName = Object.keys(jsonData)[0];
        const asset = tableName.replace('tbl_', '');

        const intervals = jsonData[tableName];
        console.log(`Started fetching data for ${asset}`);

        for (const item of intervals) {
          const minutesDifPerInterval = (item.end - item.start) / 60000;

          const batches =
            minutesDifPerInterval > batchSize
              ? createBatches(item.start, item.end, batchSize)
              : [{ start: item.start, end: item.end }];

          for (const batchItem of batches) {
            if (leftReq-- <= 0) {
              console.log(`Reached request limit, sleeping for 65 seconds`);
              await sleep(65 * 1000);
              leftReq = BinanceWeightLimit;
            }

            const fetchedCandles = await fetchCandleData(
              asset,
              interval,
              batchItem.start,
              batchItem.end
            );

            if (fetchedCandles.length === 0) {
              console.log(
                `No candles returned for ${asset} in the interval ${batchItem.start} to ${batchItem.end}`
              );
              continue;
            }

            if (currentSymbol === asset) {
              batch.push(...fetchedCandles);
            } else {
              if (currentSymbol) {
                await loadCandlesToDb(true, batch, currentSymbol);
                batch = [];
              }
              currentSymbol = asset;
              batch.push(...fetchedCandles);
            }
          }
        }

        fs.unlinkSync(filePath);
        console.log(`Deleted file: ${filePath}`);
      } catch (err) {
        console.error('Error reading or parsing file:', filePath, err);
      }
    }

    if (currentSymbol && batch.length > 0) {
      await loadCandlesToDb(true, batch, currentSymbol);
    }
  } catch (err) {
    console.error('Error reading directory:', err);
  }
}

function createBatches(start, end, batchSize) {
  console.log(`Creating batches from ${start} to ${end}`);
  const batches = [];
  for (
    let batchStart = start;
    batchStart < end;
    batchStart += batchSize * 60000
  ) {
    const batchEnd = Math.min(batchStart + batchSize * 60000, end);
    batches.push({ start: batchStart, end: batchEnd });
  }
  return batches;
}

async function fetchCandleData(
  symbol,
  interval,
  startTime,
  endTime,
  retries = 3
) {
  const baseUrl =
    market === 'binance_futures'
      ? 'https://fapi.binance.com/fapi/v1/klines'
      : 'https://api.binance.com/api/v3/klines';

  const url = `${baseUrl}?symbol=${symbol}&interval=${interval}&startTime=${startTime}&endTime=${endTime}&limit=1500`;

  console.log(`Fetching data from URL: ${url}`);

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await axios.get(url);
      if (response.data.length === 0) {
        console.log('no data fetched');
      }
      return response.data;
    } catch (error) {
      console.log(`Attempt ${attempt} failed: ${error.message}`);
      if (attempt < retries) {
        console.log(`Retrying in ${attempt * 1000} ms...`);
        await sleep(attempt * 1000);
      } else {
        console.log('All retry attempts failed.');
        return null;
      }
    }
  }
}

function sleep(ms) {
  console.log(`Sleeping for ${ms / 1000} seconds`);
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function loadCandlesToDb(isActive = true, batch, currentSymbol) {
  try {
    let tsvLinesBatch = [];
    for (const candleData of batch) {
      const ts_start = candleData[0];
      const ts_end = candleData[6];
      const open = candleData[1];
      const high = candleData[2];
      const low = candleData[3];
      const close = candleData[4];
      const volume_base = candleData[5];
      const volume_quote = candleData[7];
      const date = Date.now();
      const newLine = `${ts_start}\t${ts_end}\t${+open}\t${+high}\t${+low}\t${+close}\t${+volume_base}\t${+volume_quote}\t${date}`;
      tsvLinesBatch.push(newLine);
    }

    const tsvData = tsvLinesBatch.join('\n');
    const filePath = path.join(__dirname, 'data.tsv');
    fs.writeFileSync(filePath, tsvData);
    console.log(`Stream created for  ${currentSymbol}`);

    const tsvStream = fs.createReadStream(filePath);
    const clickhouseStream = await clickhouse.query(
      `INSERT INTO db_candles_${market}.tbl_${currentSymbol} FORMAT TSV`
    );
    tsvStream.pipe(clickhouseStream);

    console.log(`Loaded historical data for ${currentSymbol} successfully`);

    clickhouseStream.on('error', (err) => {
      console.log(`Error happened for ${currentSymbol}`);
      console.log(err.message);
    });

    clickhouseStream.on('end', (err) => {
      console.log(`Stream ended for ${currentSymbol}`);
    });
  } catch (error) {
    console.error('Error saving historical candle data:', error.message);
  }
}

async function retryClickhouseStream(tsvStream, symbol, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const clickhouseStream = await clickhouse.query(
        `INSERT INTO db_candles_${market}.tbl_${symbol} FORMAT TSV`
      );
      tsvStream.pipe(clickhouseStream);

      return new Promise((resolve, reject) => {
        clickhouseStream.on('end', async () => {
          try {
            await fs.promises.truncate(filePath, 0);
            console.log(`TSV for symbol ${symbol} successfully loaded.`);
            console.log('File content deleted');
            resolve();
          } catch (truncateError) {
            reject(truncateError);
          }
        });

        clickhouseStream.on('error', (err) => {
          console.error(`Attempt ${attempt} failed: ${err.message}`);
          tsvStream.unpipe(clickhouseStream);
          if (attempt < retries) {
            console.log(`Retrying in ${attempt * 1000} ms...`);
            setTimeout(() => {
              retryClickhouseStream(tsvStream, symbol, retries - attempt)
                .then(resolve)
                .catch(reject);
            }, attempt * 1000);
          } else {
            reject('All retry attempts failed.');
          }
        });
      });
    } catch (error) {
      console.log(`Attempt ${attempt} failed: ${error.message}`);
      if (attempt < retries) {
        console.log(`Retrying in ${attempt * 1000} ms...`);
        await sleep(attempt * 1000);
      } else {
        console.log('All retry attempts failed.');
      }
    }
  }
  return Promise.resolve(); // Ensure the promise resolves after all retries
}

async function removeDuplicates() {
  try {
    const tablesQuery = `
        SELECT name
        FROM system.tables
        WHERE database = 'db_candles_${market}'
      `;
    const tables = await executeQuery(tablesQuery);

    for (const table of tables) {
      const query = `OPTIMIZE TABLE db_candles_${market}.${table} FINAL DEDUPLICATE BY ts_start`;
      await executeQuery(query);
      console.log(`Table ${table} was deduplicated by ts_start`);

      const removeEmptyRowsQuery = `ALTER TABLE db_candles_${market}.${table} DELETE WHERE ts_start = 0`;
      await executeQuery(removeEmptyRowsQuery);
      console.log(`Empty rows removed from table ${table}`);
    }
  } catch (error) {
    console.log('ERROR', 'binance_futures.index.main', error.message);
  }
}

function convertIntervalsToLocalTime() {
  console.log(`Reading files from directory: ${missingIntervalsDir}`);

  const files = fs.readdirSync(missingIntervalsDir);
  console.log(`Found ${files.length} files.`);

  for (const file of files) {
    const filePath = path.join(missingIntervalsDir, file);
    const bigMissingPeriods = [];
    const smallMissingPeriods = [];

    const data = fs.readFileSync(filePath, 'utf8');
    const jsonData = JSON.parse(data);
    const key = Object.keys(jsonData)[0];
    const intervalsArray = jsonData[key];

    intervalsArray.forEach(({ start, end }) => {
      const differenceInHrs = (end - start) / (1000 * 60 * 60);
      if (differenceInHrs > 24) {
        const convertedStart = convertToLocalTime(start);
        const convertedEnd = convertToLocalTime(end);

        const periodInfo = {
          start: convertedStart,
          end: convertedEnd,
        };

        bigMissingPeriods.push(periodInfo);
      } else {
        const periodInfo = {
          start,
          end,
        };
        smallMissingPeriods.push(periodInfo);
      }
    });
    const bigMissingDir = path.join(__dirname, 'big_missing_period');
    const smallMissingDir = path.join(__dirname, 'small_missing_period');

    if (!fs.existsSync(bigMissingDir)) {
      fs.mkdirSync(bigMissingDir);
    }
    if (!fs.existsSync(smallMissingDir)) {
      fs.mkdirSync(smallMissingDir);
    }

    const filePathBigMisingPeriods = path.join(bigMissingDir, file);
    const filePathSmallMisingPeriods = path.join(smallMissingDir, file);

    fs.writeFileSync(
      filePathBigMisingPeriods,
      JSON.stringify(bigMissingPeriods, null, 2)
    );
    fs.writeFileSync(
      filePathSmallMisingPeriods,
      JSON.stringify(smallMissingPeriods, null, 2)
    );
  }
}

function getDelistedPairs() {
  const bigMissingDir = path.join(__dirname, 'big_missing_period');
  const files = fs.readdirSync(bigMissingDir);
  const delistedPairs = [];

  for (const file of files) {
    const filePath = path.join(bigMissingDir, file);
    const data = fs.readFileSync(filePath, 'utf8');
    const jsonData = JSON.parse(data);

    // console.log(intervalsArray);

    if (jsonData.length === 0) {
      const pairName = file
        .replace('missing_intervals_tbl_', '')
        .replace('.json', '');

      delistedPairs.push(pairName);
    }
  }

  console.log(delistedPairs);
}

function processCsvFileForManualUpload() {
  const bigMissingDir = path.join(__dirname, 'big_missing_period');
  const outputCsv = path.join(__dirname, 'missing_intervals.csv');
  const files = fs.readdirSync(bigMissingDir);
  const csvLines = [];

  for (const file of files) {
    if (file.startsWith('missing_intervals_tbl_') && file.endsWith('.json')) {
      const asset = file
        .replace('missing_intervals_tbl_', '')
        .replace('.json', '');
      const filePath = path.join(bigMissingDir, file);

      const data = fs.readFileSync(filePath, 'utf8');
      const jsonData = JSON.parse(data);

      jsonData.forEach(({ start, end }) => {
        const ts_start = convertToUnixTimestamp(start);
        const ts_end = convertToUnixTimestamp(end);
        const csvLine = `${asset},${ts_start},${ts_end},${start},${end}`;
        csvLines.push(csvLine);
      });
    }
  }

  const csvHeader = 'asset,ts_start,ts_end,start,end';
  const csvContent = [csvHeader, ...csvLines].join('\n');
  fs.writeFileSync(outputCsv, csvContent);
  console.log(`CSV file has been created: ${outputCsv}`);
}

function convertToLocalTime(timestamp) {
  return new Date(timestamp).toLocaleString(); // Uses system's local time
}

function convertToUnixTimestamp(dateString) {
  const [datePart, timePart] = dateString.split(', ');

  const [day, month, year] = datePart.split('.');

  const isoDateString = `${year}-${month}-${day}T${timePart}`;

  const date = new Date(isoDateString);
  return Math.floor(date.getTime());
}

async function uploadSingleCsvFile(filePath, asset) {
  return new Promise((resolve, reject) => {
    const readableStream = fs.createReadStream(filePath);
    const query = `INSERT INTO db_candles_${market}.tbl_${asset} (ts_start, ts_end, open, high, low, close, volume_base, volume_quote) FORMAT CSV`;

    const writableStream = clickhouse.query(query, (err, result) => {
      if (err) {
        reject(err);
      } else {
        resolve(result);
      }
    });

    readableStream.pipe(writableStream);

    readableStream.on('error', (err) => {
      reject(err);
    });

    writableStream.on('finish', () => {
      resolve();
    });

    writableStream.on('error', (err) => {
      reject(err);
    });
  });
}

async function uploadCsvFilesToDB() {
  const csvDir = path.join(__dirname, 'csv');
  console.log(`Reading files from directory: ${csvDir}`);

  const files = fs.readdirSync(csvDir);

  for (const file of files) {
    const filePath = path.join(csvDir, file);
    await processAndReorderCsv(filePath);
    const asset = file.replace('.csv', '');

    try {
      await uploadSingleCsvFile(filePath, asset);
      console.log(`Successfully uploaded file: ${file}`);
      fs.unlinkSync(filePath);
      console.log(`Successfully deleted file: ${file}`);
    } catch (err) {
      console.error(`Failed to upload file ${file}:`, err);
    }
  }

  console.log('All files have been processed.');
}

async function addTablesToDB() {
  const tablesName = getTablesName();
  for (const name of tablesName) {
    const query = `CREATE TABLE db_candles_${market}.tbl_${name} (
      ts_start      UInt64, 
      ts_end        UInt64, 
      \`open\`        Float64, 
      high          Float64, 
      low           Float64, 
      \`close\`       Float64, 
      volume_base   Float64, 
      volume_quote  Float64, 
      ts_insert     UInt64
    ) ENGINE = MergeTree() ORDER BY ts_start`;

    try {
      await executeQuery(query);
      console.log(`Table ${name} created successfully`);
    } catch (error) {
      console.error(`Failed to create table ${name}`, error);
    }
  }
}

// (async () => {
//   try {
//     await addTablesToDB();
//     console.log('Tables created successfully.');
//   } catch (error) {
//     console.error('Error creating tables:', error);
//   }
// })();

function getTablesName() {
  const csvDir = path.join(__dirname, 'csv');
  console.log(`Reading files from directory: ${csvDir}`);

  const files = fs.readdirSync(csvDir);
  const assets = [];

  for (const file of files) {
    const asset = file.replace('.csv', '');
    console.log(asset);

    assets.push(asset);
  }

  return assets;
}

async function processAndReorderCsv(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csvParser([false]))
      .on('data', (row) => {
        const reorderedRow = [
          row[Object.keys(row)[0]], // original 1st column
          row[Object.keys(row)[6]], // original 7th column
          row[Object.keys(row)[1]], // original 2nd column
          row[Object.keys(row)[2]], // original 3rd column
          row[Object.keys(row)[3]], // original 4th column
          row[Object.keys(row)[4]], // original 5th column
          row[Object.keys(row)[5]], // original 6th column
          row[Object.keys(row)[7]], // original 8th column
        ];

        results.push(reorderedRow);
      })
      .on('end', () => {
        stringify.stringify(results, { header: false }, (err, output) => {
          if (err) reject(err);
          fs.writeFileSync(filePath, output);
          resolve();
        });
      })
      .on('error', (err) => {
        reject(err);
      });
  });
}

async function removePeriodsFromDB() {
  const smallMissingDir = path.join(__dirname, 'small_missing_period');
  console.log(`Reading files from directory: ${smallMissingDir}`);
  const files = fs.readdirSync(smallMissingDir);
  for (const file of files) {
    if (file.startsWith('missing_intervals_tbl_') && file.endsWith('.json')) {
      const asset = file
        .replace('missing_intervals_tbl_', '')
        .replace('.json', '');
      const filePath = path.join(smallMissingDir, file);

      try {
        const data = fs.readFileSync(filePath, 'utf8');
        const jsonData = JSON.parse(data);

        for (const period of jsonData) {
          const { start, end } = period;
          console.log(
            `Removing records for ${asset} between ${start} and ${end}`
          );

          const deleteQuery = `
            ALTER TABLE db_candles_${market}.tbl_${asset}
            DELETE WHERE ts_start >= ${start} AND ts_start <= ${end}
          `;
          console.log(deleteQuery);

          await executeQuery(deleteQuery);
          console.log(
            `Records deleted for ${asset} between ${start} and ${end}`
          );
        }

        // Optionally, delete the file after processing
        // fs.unlinkSync(filePath);
        // console.log(`Deleted file: ${filePath}`);
      } catch (err) {
        console.error('Error reading or processing file:', filePath, err);
      }
    }
  }

  console.log('All periods have been processed.');
}

// processCsvFileForManualUpload()
// getDelistedPairs();
// getMissingIntervalsFiles();
// convertIntervalsToLocalTime();
initCandlesBackup();
// uploadCsvFilesToDB();
//  removeDuplicates();
// removePeriodsFromDB()