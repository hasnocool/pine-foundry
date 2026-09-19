// src/semantic.rs
use std::collections::HashSet;

pub const METHOD: &str = "semantic-hybrid-v1";

#[derive(Debug, Clone, Default)]
pub struct SemanticFeatures {
    pub core_terms: Vec<String>,
    pub terms: Vec<String>,
    pub fingerprint: u64,
}

pub fn features(
    title: &str,
    description: Option<&str>,
    ticker: Option<&str>,
    event_type: &str,
) -> SemanticFeatures {
    let mut tokens = tokenize(title);
    if let Some(description) = description {
        tokens.extend(tokenize(description));
    }

    let mut core_terms = tokens.iter().cloned().collect::<HashSet<_>>();
    let mut terms = core_terms.clone();
    for pair in tokens.windows(2) {
        if let [a, b] = pair {
            terms.insert(format!("{a}_{b}"));
        }
    }

    if let Some(ticker) = ticker.filter(|value| !value.trim().is_empty()) {
        terms.insert(format!("ticker:{}", ticker.trim().to_ascii_lowercase()));
    }
    if !event_type.trim().is_empty() {
        terms.insert(format!("event:{}", event_type.trim().to_ascii_lowercase()));
    }

    let mut core_terms = core_terms.drain().collect::<Vec<_>>();
    core_terms.sort();

    let mut terms = terms.into_iter().collect::<Vec<_>>();
    terms.sort();
    terms.truncate(96);

    SemanticFeatures {
        core_terms,
        fingerprint: simhash(&terms),
        terms,
    }
}

pub fn similarity(left: &SemanticFeatures, right: &SemanticFeatures) -> f64 {
    if left.terms.is_empty() || right.terms.is_empty() {
        return 0.0;
    }

    let left_core = left.core_terms.iter().collect::<HashSet<_>>();
    let right_core = right.core_terms.iter().collect::<HashSet<_>>();
    let core_intersection = left_core.intersection(&right_core).count() as f64;
    let core_union = left_core.union(&right_core).count() as f64;
    let core_jaccard = if core_union > 0.0 {
        core_intersection / core_union
    } else {
        0.0
    };

    let left_terms = left.terms.iter().collect::<HashSet<_>>();
    let right_terms = right.terms.iter().collect::<HashSet<_>>();
    let intersection = left_terms.intersection(&right_terms).count() as f64;
    let union = left_terms.union(&right_terms).count() as f64;
    let jaccard = if union > 0.0 { intersection / union } else { 0.0 };

    let hamming = (left.fingerprint ^ right.fingerprint).count_ones() as f64;
    let fingerprint_similarity = 1.0 - hamming / 64.0;

    (0.60 * core_jaccard + 0.25 * jaccard + 0.15 * fingerprint_similarity)
        .clamp(0.0, 1.0)
}

pub fn cluster_id(features: &SemanticFeatures) -> String {
    format!("semantic-{:016x}", features.fingerprint)
}

fn tokenize(text: &str) -> Vec<String> {
    const STOP: &[&str] = &[
        "the", "a", "an", "of", "to", "for", "and", "or", "on", "in", "with", "by",
        "from", "at", "as", "is", "are", "was", "were", "be", "been", "this", "that",
        "these", "those", "it", "its", "into", "about", "after", "before", "during",
        "over", "under", "new", "more", "latest", "today", "says", "said", "announce",
        "announces", "announced", "update", "news", "company", "corporation", "inc",
        "corp", "ltd", "limited", "shares", "stock", "market",
    ];

    text.split_whitespace()
        .filter_map(|raw| {
            let mut token = raw
                .chars()
                .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '$')
                .collect::<String>()
                .to_ascii_lowercase();
            if token.starts_with('$') {
                token = token.trim_start_matches('$').to_string();
            }
            if token.len() < 3 || STOP.contains(&token.as_str()) {
                return None;
            }

            token = normalize_synonym(token);
            token = simple_stem(token);
            Some(token)
        })
        .collect()
}

fn normalize_synonym(token: String) -> String {
    match token.as_str() {
        "acquire" | "acquired" | "acquires" | "acquiring" | "acquisition"
        | "takeover" | "merger" | "merges" | "buy" | "buys" | "bought"
        | "purchase" | "purchases" | "purchased" | "deal" | "transaction" => "acquisition".into(),
        "earnings" | "earning" | "eps" | "revenue" | "profit" | "profits" | "loss" | "losses" => "earnings".into(),
        "guidance" | "outlook" | "forecast" | "forecasts" => "guidance".into(),
        "offering" | "offerings" | "dilution" | "dilutive" | "atm" => "offering".into(),
        "buyback" | "buybacks" | "repurchase" | "repurchases" => "buyback".into(),
        "dividend" | "dividends" | "distribution" | "distributions" => "dividend".into(),
        "fda" | "approval" | "approved" | "clinical" | "trial" | "trials" => "clinical".into(),
        "lawsuit" | "litigation" | "investigation" | "probe" | "regulatory" => "regulatory".into(),
        "bankruptcy" | "restructuring" => "bankruptcy".into(),
        "ceo" | "chief" | "executive" | "management" => "management".into(),
        "contract" | "contracts" | "award" | "partnership" | "partnerships" => "commercial".into(),
        "hack" | "hacked" | "exploit" | "exploited" | "breach" | "breached" => "security".into(),
        "etf" | "fund" | "funds" => "fund".into(),
        _ => token,
    }
}

fn simple_stem(mut token: String) -> String {
    for suffix in ["ingly", "edly", "ing", "ed", "es"] {
        if token.len() > suffix.len() + 3 && token.ends_with(suffix) {
            token.truncate(token.len() - suffix.len());
            return token;
        }
    }
    if token.len() > 4 && token.ends_with('s') {
        token.pop();
    }
    token
}

fn simhash(terms: &[String]) -> u64 {
    let mut weights = [0_i32; 64];

    for term in terms {
        let hash = fnv1a64(term.as_bytes());
        for bit in 0..64 {
            if ((hash >> bit) & 1) == 1 {
                weights[bit] += 1;
            } else {
                weights[bit] -= 1;
            }
        }
    }

    weights.iter().enumerate().fold(0_u64, |fingerprint, (bit, weight)| {
        if *weight >= 0 {
            fingerprint | (1_u64 << bit)
        } else {
            fingerprint
        }
    })
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn related_headlines_score_higher_than_unrelated() {
        let a = features(
            "Acme acquires biotech startup for $500 million",
            Some("The acquisition expands the company's clinical pipeline"),
            Some("ACME"),
            "m_and_a",
        );
        let b = features(
            "Acme to buy biotech firm in $500 million deal",
            Some("The transaction expands the drug development pipeline"),
            Some("ACME"),
            "m_and_a",
        );
        let c = features(
            "Oil prices fall as central bank changes interest rate outlook",
            None,
            None,
            "macro",
        );

        assert!(similarity(&a, &b) > similarity(&a, &c));
        assert!(similarity(&a, &b) >= 0.55);
    }
}
