# News ingestion

Pine Foundry treats news as a side-channel around the market-event engine rather than putting HTTP news calls in the market-data hot path.

## Sources

1. Reddit global search RSS
   - https://www.reddit.com/search.rss?q={query}&sort=new&limit={n}
2. Reddit global search JSON
   - https://www.reddit.com/search.json?q={query}&sort=new&limit={n}
3. Google News RSS search
   - https://news.google.com/rss/search?q={query}&hl=en-CA&gl=CA&ceid=CA:en
4. NewsAPI Everything
   - https://newsapi.org/v2/everything

Every source is normalized into NewsArticle.

## Ticker searches

GET /api/news/ticker/:ticker builds a broad query from the ticker. Known crypto tickers add common asset-name aliases such as Bitcoin, Ethereum and Solana.

Example:

~~~text
/api/news/ticker/BTC
~~~

For a stock ticker or unknown symbol, Pine Foundry searches both the dollar-prefixed symbol and plain symbol.

Free-form searches can be supplied with:

~~~text
/api/news/search?q=quantum%20computing&ticker=XYZ&limit=25
~~~

## NewsAPI credential

Set either:

~~~text
NEWSAPI=...
~~~

or:

~~~text
NEWSAPI_KEY=...
~~~

Pine Foundry sends the credential through X-Api-Key rather than putting it into the URL. Credentials are excluded from source control by .gitignore.

## Freshness

NewsAPI requests include a configurable from timestamp:

~~~text
PINE_FOUNDRY_NEWS_LOOKBACK_HOURS=24
~~~

Reddit and Google News queries request newest-first feeds. Results are normalized, deduplicated by URL and sorted newest first.

## Automatic worker

The default automatic worker refreshes:

~~~text
PINE_FOUNDRY_NEWS_TICKERS=BTC,ETH,SOL
PINE_FOUNDRY_NEWS_QUERIES=
PINE_FOUNDRY_NEWS_POLL_SECS=60
PINE_FOUNDRY_NEWS_LIMIT=25
PINE_FOUNDRY_NEWS_CONCURRENCY=4
~~~

Use PINE_FOUNDRY_NEWS_QUERIES for additional broad monitoring phrases. Search jobs are concurrency-limited so an aggressive ticker list does not overwhelm providers.

## Cache

Recent normalized articles are retained in an in-memory bounded cache:

~~~text
PINE_FOUNDRY_NEWS_CACHE_SIZE=500
~~~

The cache is deliberately volatile. It is not a durable historical news database yet.

## Failure behavior

Unified search attempts all sources. Individual provider failures are recorded in /api/news/health and do not suppress results from working providers.

Reddit RSS is retained as a keyless fallback when anonymous JSON access is unavailable. Public feeds can still be throttled, so the automatic worker defaults to a 60-second cadence rather than tight polling.
