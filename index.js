const ClickHouse = require('@apla/clickhouse');
const axios = require('axios');
const dotenv = require('dotenv');
const fs = require('fs');
const path = require('path');
const retry = require('retry');

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
  if (!fs.existsSync(missingIntervalsDir)) {
    fs.mkdirSync(missingIntervalsDir);
    console.log(`Directory created: ${missingIntervalsDir}`);
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
    const tablesQuery = `
        SELECT name
        FROM system.tables
        WHERE database = 'db_candles_${market}'
      `;
    const tables = await executeQuery(tablesQuery);
    if (!fs.existsSync(missingIntervalsDir)) {
      fs.mkdirSync(missingIntervalsDir);
    }

    for (const table of tables) {
      const tableName = table[0];

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
  const url = `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=${interval}&startTime=${startTime}&endTime=${endTime}&limit=1500`;
  console.log(`Fetching data from URL: ${url}`);

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await axios.get(url);
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

function uploadSingleCsvFile(filePath, asset) {
  return new Promise((resolve, reject) => {
    const readableStream = fs.createReadStream(filePath);
    const query = `INSERT INTO db_candles_${market}.tbl_${asset} FORMAT CSV`;

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
  console.log(files);

  for (const file of files) {
    const filePath = path.join(csvDir, file);
    const asset = file.replace('.csv', '');

    try {
      await uploadSingleCsvFile(filePath, asset);
      console.log(`Successfully uploaded file: ${file}`);
    } catch (err) {
      console.error(`Failed to upload file ${file}:`, err);
    }
  }

  console.log('All files have been processed.');
}

// processCsvFileForManualUpload()
// getDelistedPairs();
// getMissingIntervalsFiles()
// convertIntervalsToLocalTime();
// initCandlesBackup();
uploadCsvFilesToDB();
