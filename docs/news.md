# News and catalyst ingestion

Pine Foundry treats news as a side-channel around the market-event engine and turns fresh stories into deterministic CatalystEvent records.

## Sources

1. Reddit global search RSS
   - https://www.reddit.com/search.rss?q={query}&sort=new&limit={n}
2. Reddit global search JSON
   - https://www.reddit.com/search.json?q={query}&sort=new&limit={n}
3. Google News RSS search
   - https://news.google.com/rss/search?q={query}&hl=en-CA&gl=CA&ceid=CA:en
4. NewsAPI Everything
   - https://newsapi.org/v2/everything

Additional regulatory/disclosure sources:
- SEC EDGAR submissions
- Canadian public disclosure discovery restricted to SEDAR+ and TSX domains through Google News RSS

## Search model

Ticker searches expand known assets:

~~~text
BTC -> $BTC OR BTC OR Bitcoin
ETH -> $ETH OR ETH OR Ethereum
SOL -> $SOL OR SOL OR Solana
~~~

Unknown symbols search both dollar-prefixed and plain forms.

Free-form searches use:

~~~text
/api/news/search?q=quantum%20computing&limit=25
~~~

The browser UI chooses ticker expansion for ticker-like strings and broad search for normal phrases.

## NewsAPI guard

NEWSAPI or NEWSAPI_KEY is accepted from the process environment.

Local guards:

~~~text
PINE_FOUNDRY_NEWSAPI_MIN_INTERVAL_SECS=1800
PINE_FOUNDRY_NEWSAPI_DAILY_LIMIT=90
~~~

The guard is process-local and intentionally conservative so a large ticker universe cannot consume the API allowance on every polling loop.

## Fresh-story processing

Background polling keeps a process-level seen set so the same URL is not repeatedly converted into CatalystEvent records.

Each fresh article is:
1. normalized
2. classified
3. assigned a story-cluster ID
4. journaled
5. attached to the related SecurityState when its ticker exists

## Catalyst classification

The deterministic classifier recognizes:

~~~text
m_and_a
earnings
guidance
offering
buyback
dividend
fda_or_clinical
legal_or_regulatory
bankruptcy
management
contract_or_partnership
security_incident
etf
macro
general
~~~

This classification is intentionally cheap and explainable. LLM enrichment belongs downstream.

## Story clustering

Headlines are normalized into important tokens, sorted and hashed into a stable cluster ID.

Each StoryCluster retains:
- canonical title
- first/last seen
- article count
- source count/list
- ticker count/list
- event type

The cluster cache is bounded.

## News velocity

SecurityState maintains one-hour news history and computes:
- article count in 5m
- article count in 15m
- relative news velocity
- unique source count in 15m

These are scanner fields and can be filtered/sorted.

## Canadian disclosure discovery

The Canadian adapter uses public Google News RSS with:

~~~text
site:sedarplus.ca
site:sedarplus.ca/csa-party
site:tsx.com/en/news
~~~

Configured tickers:

~~~text
PINE_FOUNDRY_CANADA_TICKERS=SHOP,RY,ABX
PINE_FOUNDRY_CANADA_POLL_SECS=300
~~~

The adapter intentionally does not depend on undocumented SEDAR+ internal JSON endpoints.

## SEC filings

SEC configuration:

~~~text
PINE_FOUNDRY_SEC_TICKERS=AAPL,MSFT
PINE_FOUNDRY_SEC_POLL_SECS=30
PINE_FOUNDRY_SEC_USER_AGENT=PineFoundry/0.4 research
~~~

Filing records are normalized into FilingEvent and classified by form, then added to the symbol catalyst history.

## API surfaces

~~~text
GET /api/news/search?q=...
GET /api/news/ticker/:ticker
GET /api/news/cache
GET /api/news/clusters
GET /api/news/health
GET /api/filings/:ticker
GET /api/canada/disclosures/:ticker
GET /api/catalysts/:ticker
GET /api/evidence/:ticker
~~~

## Replay

News, Canadian disclosures and SEC filings are written into the event journal and replayed through the same state/catalyst engine.

~~~text
cargo run -- replay YYYY-MM-DD
~~~


## Configurable entity aliases

Add company or asset-name aliases without changing code:

~~~text
PINE_FOUNDRY_NEWS_ALIASES=AAPL=Apple|Apple Inc;MSFT=Microsoft|Microsoft Corporation
~~~

Each entry is `TICKER=alias1|alias2`. These aliases are added to the normal `$TICKER OR TICKER` search query for Reddit, Google News and NewsAPI.
