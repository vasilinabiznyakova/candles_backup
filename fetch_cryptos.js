const axios = require('axios');
const fs = require('fs');

async function getTopCryptos(limit = 200) {
  const url =
    'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing';
  const params = {
    start: 1,
    limit: limit,
    sortBy: 'market_cap',
    sortType: 'desc',
    // convert: 'USD',
    cryptoType: 'all',
    tagType: 'all',
    audited: false,
    aux: 'market_cap,cmc_rank',
  };

  try {
    const response = await axios.get(url, { params });
    const symbols = response.data.data.cryptoCurrencyList.map(
      (crypto) => crypto.symbol
    );
    return symbols;
  } catch (error) {
    console.error('Error fetching top cryptos:', error.message);
    return [];
  }
}

async function filterSpotTradingPairs(symbols) {
  const url = 'https://api.binance.com/api/v3/exchangeInfo';

  try {
    const response = await axios.get(url);
    const spotPairs = response.data.symbols
      .filter((symbolInfo) => symbolInfo.isSpotTradingAllowed)
      .map((symbolInfo) => symbolInfo.symbol);

    const spotTradingSymbols = symbols.filter((symbol) =>
      spotPairs.some((pair) => pair.includes(symbol))
    );
    return spotTradingSymbols;
  } catch (error) {
    console.error('Error fetching Binance spot trading pairs:', error.message);
    return [];
  }
}

async function main() {
  const topCryptos = await getTopCryptos(200); 
  const spotCryptos = await filterSpotTradingPairs(topCryptos);

  const top100SpotCryptos = spotCryptos.slice(0, 101);

  fs.writeFileSync('assets.json', JSON.stringify(top100SpotCryptos, null, 2));
  console.log('Top 100 spot trading cryptos saved to assets.json');
}

main().catch(console.error);
