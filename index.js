const ClickHouse = require('@apla/clickhouse');
const dotenv = require('dotenv');
const fs = require('fs');
const path = require('path');


dotenv.config();

const clickhouse = new ClickHouse({
  host: process.env.CH_HOST,
  port: process.env.CH_PORT,
  user: process.env.CH_USER,
  password: process.env.CH_PASSWORD,
  timeout: 60000,
  max_open_connections: 20,
});


function executeQuery(query) {
  return new Promise((resolve, reject) => {
    const rows = [];
    const stream = clickhouse.query(query);
    
    stream.on('data', (row) => rows.push(row));
    stream.on('end', () => resolve(rows));
    stream.on('error', (err) => reject(err));
  });
}

const market = process.env.MARKET.toLowerCase();

async function main() {
  try {
    const tablesQuery = `
      SELECT name
      FROM system.tables
      WHERE database = 'db_candles_${market}'
    `;
      const tables = await executeQuery(tablesQuery);
    //   console.log(tables);

    const result = {};

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
           __dirname,
           `missing_intervals_${tableName}.json`
         );
         fs.writeFileSync(filePath, JSON.stringify(result, null, 2));
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
    // console.log(data[0]);
    const missingIntervals = [];
    
   for (let i = 1; i < data.length; i++) {
     const prevTimestamp = Number(data[i - 1]);
     const currentTimestamp = Number(data[i]);
     const timeDiff = currentTimestamp - prevTimestamp;
     if (timeDiff > 60000) {
         const startInterval = prevTimestamp + 60000;
         const finishInterval = currentTimestamp - 60000; 
       missingIntervals.push({
         start: startInterval,
         end: finishInterval,
       });
     }
    }
    // console.log(missingIntervals);
    return missingIntervals;
}

main();
