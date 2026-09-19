# Issuer-specific feeds

Pine Foundry can ingest issuer-specific RSS/Atom feeds without adding provider
logic to the scanner core.

Configure one or more official or authorized feeds:

~~~text
PINE_FOUNDRY_ISSUER_FEEDS=AAPL=https://example.com/aapl/rss.xml;MSFT=https://example.com/msft/atom.xml
~~~

Entries are separated with semicolons. Each entry is TICKER=URL.

The feed is normalized into the existing NewsArticle model, assigned a
deterministic catalyst type and semantic story cluster, and then journaled as
a normal news event. The market scanner does not need to know where the article
originated.

## Sources to prefer

### SEC EDGAR company-search RSS

The SEC explicitly documents RSS subscriptions for EDGAR Company Search
results. A user can open Company Search, select the issuer, apply filing-type
filters, and copy the RSS URL exposed by the results page.

Reference:

https://www.sec.gov/about/rss-feeds

https://www.sec.gov/search-filings

This is the preferred issuer-specific source for U.S. regulatory filings when
the existing SEC submissions adapter is not the appropriate shape for a
specific workflow.

### Issuer investor-relations RSS/Atom

Many issuers publish RSS/Atom links from their Investor Relations pages.
Prefer the feed URL published by the issuer rather than scraping an HTML page.

Record the exact feed URL in PINE_FOUNDRY_ISSUER_FEEDS and keep a note in your
deployment configuration about the issuer page that documents it.

### GlobeNewswire direct-from-source corporate news

GlobeNewswire documents RSS feeds for press-release content and describes its
release stream as direct-from-source corporate news issued by its customers.

https://www.globenewswire.com/newswire-press-release-content

When an issuer's GlobeNewswire presence exposes a feed suitable for that
issuer, configure that URL directly instead of broad keyword search.

### Business Wire issuer pages

Business Wire issuer/release pages expose a Get RSS Feed action for company
news. Use the feed URL exposed by the issuer/newsroom page and configure it as
an issuer feed.

## Operational rules

Issuer feeds are intentionally configuration-driven:

- no undocumented endpoint assumptions;
- no credentials in source control;
- provider failures are isolated through the normal news health path;
- feeds are fetched outside the market tick path;
- the same URL/article deduplication applies;
- the same semantic clusterer is used across issuer feeds, Reddit, Google News
  and NewsAPI;
- feed terms, rate limits and licensing remain the operator's responsibility.

The existing SEC and Canadian disclosure adapters remain in place. Issuer RSS
is an additive path for public feeds whose contracts are documented by the
issuer or feed provider.
