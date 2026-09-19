// src/orderbook.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: f64,
    pub quantity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrderBookState {
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub sequence: Option<u64>,
    pub last_update_ms: i64,
    pub valid: bool,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct BookMetrics {
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub spread: Option<f64>,
    pub spread_bps: Option<f64>,
    pub mid: Option<f64>,
    pub microprice: Option<f64>,
    pub bid_depth_5: f64,
    pub ask_depth_5: f64,
    pub bid_depth_10: f64,
    pub ask_depth_10: f64,
    pub book_imbalance: Option<f64>,
    pub liquidity_score: f64,
    pub executable_buy_1000: f64,
    pub executable_sell_1000: f64,
}

impl OrderBookState {
    pub fn replace(
        &mut self,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        sequence: Option<u64>,
        ts_ms: i64,
    ) {
        self.bids = normalize(bids, BookSide::Bid);
        self.asks = normalize(asks, BookSide::Ask);
        self.sequence = sequence;
        self.last_update_ms = ts_ms;
        self.valid = !self.bids.is_empty() || !self.asks.is_empty();
    }

    pub fn apply_update(
        &mut self,
        bids: &[BookLevel],
        asks: &[BookLevel],
        sequence: Option<u64>,
        ts_ms: i64,
    ) -> bool {
        self.apply_update_range(bids, asks, sequence, sequence, ts_ms)
    }

    pub fn apply_update_range(
        &mut self,
        bids: &[BookLevel],
        asks: &[BookLevel],
        first_sequence: Option<u64>,
        last_sequence: Option<u64>,
        ts_ms: i64,
    ) -> bool {
        if let (Some(first), Some(last)) = (first_sequence, last_sequence) {
            if last < first {
                return false;
            }
            if let Some(previous) = self.sequence {
                if last <= previous {
                    return false;
                }
                if first > previous.saturating_add(1) {
                    self.valid = false;
                    self.last_update_ms = ts_ms;
                    self.sequence = Some(last);
                    return false;
                }
            }
        }

        apply_levels(&mut self.bids, bids, BookSide::Bid);
        apply_levels(&mut self.asks, asks, BookSide::Ask);
        self.sequence = last_sequence.or(self.sequence);
        self.last_update_ms = ts_ms;
        self.valid = !self.bids.is_empty() && !self.asks.is_empty();
        true
    }

    pub fn metrics(&self) -> BookMetrics {
        let best_bid = self.bids.first().map(|x| x.price);
        let best_ask = self.asks.first().map(|x| x.price);
        let spread = match (best_bid, best_ask) {
            (Some(bid), Some(ask)) if ask >= bid => Some(ask - bid),
            _ => None,
        };
        let mid = match (best_bid, best_ask) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            _ => best_bid.or(best_ask),
        };
        let spread_bps = match (spread, mid) {
            (Some(value), Some(mid)) if mid > 0.0 => Some(value / mid * 10_000.0),
            _ => None,
        };

        let bid_depth_5 = self.bids.iter().take(5).map(|x| x.quantity).sum();
        let ask_depth_5 = self.asks.iter().take(5).map(|x| x.quantity).sum();
        let bid_depth_10 = self.bids.iter().take(10).map(|x| x.quantity).sum();
        let ask_depth_10 = self.asks.iter().take(10).map(|x| x.quantity).sum();
        let total = bid_depth_10 + ask_depth_10;

        let book_imbalance = if total > 0.0 {
            Some((bid_depth_10 - ask_depth_10) / total)
        } else {
            None
        };

        let microprice = match (best_bid, best_ask) {
            (Some(bid), Some(ask)) => {
                let bid_qty = self.bids.first().map(|x| x.quantity).unwrap_or(0.0);
                let ask_qty = self.asks.first().map(|x| x.quantity).unwrap_or(0.0);
                let denom = bid_qty + ask_qty;
                if denom > 0.0 {
                    Some((ask * bid_qty + bid * ask_qty) / denom)
                } else {
                    mid
                }
            }
            _ => mid,
        };

        BookMetrics {
            best_bid,
            best_ask,
            spread,
            spread_bps,
            mid,
            microprice,
            bid_depth_5,
            ask_depth_5,
            bid_depth_10,
            ask_depth_10,
            book_imbalance,
            liquidity_score: total,
            executable_buy_1000: executable_quantity(&self.asks, 1000.0),
            executable_sell_1000: executable_quantity(&self.bids, 1000.0),
        }
    }

    pub fn top_levels(&self, depth: usize) -> (Vec<BookLevel>, Vec<BookLevel>) {
        (
            self.bids.iter().take(depth).cloned().collect(),
            self.asks.iter().take(depth).cloned().collect(),
        )
    }
}

fn executable_quantity(levels: &[BookLevel], budget: f64) -> f64 {
    if budget <= 0.0 { return 0.0; }
    let mut remaining = budget;
    let mut quantity = 0.0;
    for level in levels {
        let notional = level.price * level.quantity;
        if notional <= remaining {
            quantity += level.quantity;
            remaining -= notional;
        } else {
            quantity += remaining / level.price;
            break;
        }
    }
    quantity
}

fn normalize(mut levels: Vec<BookLevel>, side: BookSide) -> Vec<BookLevel> {
    levels.retain(|x| x.price.is_finite() && x.quantity.is_finite() && x.price > 0.0 && x.quantity > 0.0);
    levels.sort_by(|a, b| {
        let order = a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal);
        match side {
            BookSide::Bid => order.reverse(),
            BookSide::Ask => order,
        }
    });
    levels.truncate(100);
    levels
}

fn apply_levels(levels: &mut Vec<BookLevel>, updates: &[BookLevel], side: BookSide) {
    for update in updates {
        if !update.price.is_finite() || !update.quantity.is_finite() || update.price <= 0.0 {
            continue;
        }
        if let Some(existing) = levels.iter_mut().find(|x| x.price == update.price) {
            if update.quantity <= 0.0 {
                existing.quantity = 0.0;
            } else {
                existing.quantity = update.quantity;
            }
        } else if update.quantity > 0.0 {
            levels.push(update.clone());
        }
    }
    levels.retain(|x| x.quantity > 0.0);
    levels.sort_by(|a, b| {
        let order = a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal);
        match side {
            BookSide::Bid => order.reverse(),
            BookSide::Ask => order,
        }
    });
    levels.truncate(100);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_book_metrics() {
        let mut book = OrderBookState::default();
        book.replace(
            vec![
                BookLevel { price: 99.0, quantity: 10.0 },
                BookLevel { price: 98.0, quantity: 20.0 },
            ],
            vec![
                BookLevel { price: 101.0, quantity: 5.0 },
                BookLevel { price: 102.0, quantity: 15.0 },
            ],
            Some(1),
            10,
        );
        let metrics = book.metrics();
        assert_eq!(metrics.best_bid, Some(99.0));
        assert_eq!(metrics.best_ask, Some(101.0));
        assert!(metrics.spread_bps.unwrap() > 0.0);
        assert!(metrics.book_imbalance.unwrap() > 0.0);
    }

    #[test]
    fn detects_sequence_gap() {
        let mut book = OrderBookState::default();
        book.replace(
            vec![BookLevel { price: 99.0, quantity: 1.0 }],
            vec![BookLevel { price: 101.0, quantity: 1.0 }],
            Some(10),
            1,
        );
        assert!(!book.apply_update(
            &[BookLevel { price: 99.5, quantity: 1.0 }],
            &[BookLevel { price: 100.5, quantity: 1.0 }],
            Some(12),
            2,
        ));
        assert!(!book.valid);
    }

    #[test]
    fn rejects_stale_sequences() {
        let mut book = OrderBookState::default();
        book.replace(vec![], vec![], Some(10), 1);
        assert!(!book.apply_update(&[], &[], Some(10), 2));
    }
}
