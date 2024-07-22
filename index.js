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
      const market = process.env.MARKET.toLowerCase();

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
          console
            .log
            // `Missing intervals for ${tableName} have been saved to ${filePath}`
            ();
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

async function readFilesFromDirectory(directory) {
  let currentSymbol = null;
  let batch = [];

  try {
    const files = fs.readdirSync(directory);

    for (const file of files) {
      const filePath = path.join(directory, file);
      try {
        const data = fs.readFileSync(filePath, 'utf8');
        const jsonData = JSON.parse(data);
        const tableName = Object.keys(jsonData)[0];
        const asset = tableName.replace('tbl_', '');

        const intervals = jsonData[tableName];
        let leftReq = BinanceWeightLimit;
           console.log(`Started fetching data for ${asset}`);

        for (const item of intervals) {
          if (leftReq-- > 0) {
            // console.log(leftReq);
            
            const fetchedCandles = await fetchCandleData(
              asset,
              interval,
              item.start,
              item.end
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
          } else {
            await sleep(65 * 1000);
            leftReq = BinanceWeightLimit;
          }
        }
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

async function fetchCandleData(
  symbol,
  interval,
  startTime,
  endTime,
  retries = 3
) {
  const url = `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=${interval}&startTime=${startTime}&endTime=${endTime}&limit=1500`;


  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await axios.get(url);
      // console.log('req done', url);
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
    // await retryClickhouseStream(tsvStream, currentSymbol);
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

readFilesFromDirectory(missingIntervalsDir);

// getMissingIntervalsFiles();