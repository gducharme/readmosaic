from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from ._artifacts import read_json, write_json_artifact

TOKEN_RE = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?")
MIN_PARAGRAPH_TOKENS = 8
CONTRACTION_PARTS = {"s", "t", "re", "ve", "ll", "d", "m"}
STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "to", "of", "in", "on", "at", "for", "with",
    "as", "is", "are", "was", "were", "be", "been", "it", "that", "this", "by", "from",
    "i", "you", "he", "she", "they", "we", "me", "my", "our", "your", "their", "his", "her",
    "its", "so", "if", "then", "than", "very", "just", "into", "out", "over", "under", "up",
    "down", "off", "about", "before", "after", "again", "already", "first", "back", "not",
    "did", "does", "do", "done", "can", "could", "would", "should", "will", "shall", "have",
    "has", "had", "because", "while", "through", "against", "inside", "outside",
    "like", "what", "said", "them", "each", "one", "other", "sometimes", "didnt", "dont",
    "cant", "wont", "youre", "thats", "theres",
}
THEME_BLOCKLIST = {
    "about", "before", "after", "already", "first", "just", "because", "through", "inside",
    "against", "forward", "back", "really", "maybe", "still", "around",
}


def _normalize_token(token: str) -> str:
    cleaned = token.lower().replace("'", "")
    cleaned = re.sub(r"[^a-z]", "", cleaned)
    return cleaned


def _tokenize(text: str) -> list[str]:
    tokens = [_normalize_token(token) for token in TOKEN_RE.findall(text)]
    return [token for token in tokens if len(token) >= 2 and token not in CONTRACTION_PARTS]


def _content_tokens(tokens: list[str]) -> list[str]:
    return [token for token in tokens if token not in STOPWORDS and len(token) >= 3]


def _entropy(tokens: list[str]) -> float:
    if not tokens:
        return 0.0
    counts = Counter(tokens)
    total = len(tokens)
    score = 0.0
    for count in counts.values():
        probability = count / total
        score -= probability * math.log2(probability)
    return round(score, 4)


def _bigrams(tokens: list[str]) -> set[tuple[str, str]]:
    return {(tokens[idx], tokens[idx + 1]) for idx in range(len(tokens) - 1)}


def _jaccard(left: set[Any], right: set[Any]) -> float:
    if not left or not right:
        return 0.0
    intersection = left.intersection(right)
    union = left.union(right)
    return len(intersection) / len(union) if union else 0.0


def _semantic_repetition(paragraphs: list[str]) -> dict[str, Any]:
    paragraph_tokens = [_content_tokens(_tokenize(paragraph)) for paragraph in paragraphs]
    token_sets = [set(tokens) for tokens in paragraph_tokens]
    bigram_sets = [_bigrams(tokens) for tokens in paragraph_tokens]

    similar_pairs: list[tuple[int, int, float]] = []
    for left in range(len(paragraphs)):
        if len(paragraph_tokens[left]) < MIN_PARAGRAPH_TOKENS:
            continue
        for right in range(left + 1, len(paragraphs)):
            if len(paragraph_tokens[right]) < MIN_PARAGRAPH_TOKENS:
                continue
            token_sim = _jaccard(token_sets[left], token_sets[right])
            bigram_sim = _jaccard(bigram_sets[left], bigram_sets[right])
            combined = (token_sim * 0.7) + (bigram_sim * 0.3)
            if combined >= 0.58 and len(token_sets[left].intersection(token_sets[right])) >= 5:
                similar_pairs.append((left, right, round(combined, 4)))

    parent = list(range(len(paragraphs)))

    def find(item: int) -> int:
        while parent[item] != item:
            parent[item] = parent[parent[item]]
            item = parent[item]
        return item

    def union(left: int, right: int) -> None:
        root_left = find(left)
        root_right = find(right)
        if root_left != root_right:
            parent[root_right] = root_left

    for left, right, _ in similar_pairs:
        union(left, right)

    clusters: dict[int, list[int]] = defaultdict(list)
    for idx in range(len(paragraphs)):
        clusters[find(idx)].append(idx)
    cluster_rows = [members for members in clusters.values() if len(members) > 1]

    top_clusters = []
    for members in cluster_rows:
        member_ids = [f"p-{member + 1:04d}" for member in sorted(members)]
        shared = Counter()
        for member in members:
            shared.update(paragraph_tokens[member])
        shared_tokens = [token for token, count in shared.items() if count >= 2][:6]
        top_clusters.append(
            {
                "paragraph_ids": member_ids,
                "cluster_size": len(members),
                "shared_tokens": shared_tokens,
                "example": paragraphs[members[0]][:220],
            }
        )
    top_clusters.sort(key=lambda row: row["cluster_size"], reverse=True)

    highlights = []
    if top_clusters:
        highlights.append(
            {
                "kind": "semantic_repetition",
                "severity": "medium",
                "message": f"{len(top_clusters)} near-duplicate paragraph clusters detected.",
                "anchors": {"paragraph_id": top_clusters[0]["paragraph_ids"][0]},
            }
        )

    return {
        "summary": {
            "paragraph_count": len(paragraphs),
            "similar_pair_count": len(similar_pairs),
            "repeated_cluster_count": len(top_clusters),
        },
        "top_repeats": [
            {
                "text": row["example"],
                "count": row["cluster_size"],
            }
            for row in top_clusters[:12]
        ],
        "top_clusters": top_clusters[:12],
        "highlights": highlights,
    }


def _signal_density(paragraphs: list[str]) -> dict[str, Any]:
    rows = []
    for idx, paragraph in enumerate(paragraphs, start=1):
        tokens = _tokenize(paragraph)
        lexical = _content_tokens(tokens)
        density = (len(lexical) / len(tokens)) if tokens else 0.0
        rows.append(
            {
                "paragraph_id": f"p-{idx:04d}",
                "token_count": len(tokens),
                "lexical_density": round(density, 4),
            }
        )
    avg = round(sum(row["lexical_density"] for row in rows) / len(rows), 4) if rows else 0.0
    low_rows = [row for row in rows if row["token_count"] >= MIN_PARAGRAPH_TOKENS and row["lexical_density"] < 0.45]
    low_rows.sort(key=lambda row: row["lexical_density"])
    highlights = []
    if low_rows:
        highlights.append(
            {
                "kind": "signal_density",
                "severity": "medium",
                "message": f"{len(low_rows)} paragraphs have low lexical density (<0.45).",
                "anchors": {"paragraph_id": low_rows[0]["paragraph_id"]},
            }
        )
    return {
        "summary": {
            "average_lexical_density": avg,
            "low_density_paragraph_count": len(low_rows),
        },
        "paragraphs": rows,
        "lowest_density_paragraphs": low_rows[:12],
        "highlights": highlights,
    }


def _surprisal(paragraphs: list[str], tokens: list[str]) -> dict[str, Any]:
    token_counts = Counter(tokens)
    total = len(tokens) or 1
    paragraph_rows = []
    skipped_short = 0
    for idx, paragraph in enumerate(paragraphs, start=1):
        paragraph_tokens = _tokenize(paragraph)
        if not paragraph_tokens:
            paragraph_rows.append({"paragraph_id": f"p-{idx:04d}", "token_count": 0, "surprisal": 0.0, "adjusted_surprisal": 0.0})
            continue
        scores = []
        for token in paragraph_tokens:
            probability = max(token_counts[token] / total, 1 / total)
            scores.append(-math.log2(probability))
        raw = sum(scores) / len(scores)
        length_weight = math.sqrt(len(paragraph_tokens) / (len(paragraph_tokens) + 12))
        adjusted = raw * length_weight
        if len(paragraph_tokens) < MIN_PARAGRAPH_TOKENS:
            skipped_short += 1
        paragraph_rows.append(
            {
                "paragraph_id": f"p-{idx:04d}",
                "token_count": len(paragraph_tokens),
                "surprisal": round(raw, 4),
                "adjusted_surprisal": round(adjusted, 4),
            }
        )

    ranked = [row for row in paragraph_rows if row["token_count"] >= MIN_PARAGRAPH_TOKENS]
    ranked.sort(key=lambda row: row["adjusted_surprisal"], reverse=True)
    mean = round(sum(row["adjusted_surprisal"] for row in ranked) / len(ranked), 4) if ranked else 0.0
    highlights = []
    if ranked:
        top = ranked[0]
        highlights.append(
            {
                "kind": "surprisal",
                "severity": "low",
                "message": f"Highest adjusted surprisal is {top['paragraph_id']} ({top['adjusted_surprisal']}).",
                "anchors": {"paragraph_id": top["paragraph_id"]},
            }
        )
    return {
        "summary": {
            "max_adjusted_surprisal": ranked[0]["adjusted_surprisal"] if ranked else 0.0,
            "mean_adjusted_surprisal": mean,
            "short_paragraphs_excluded_from_ranking": skipped_short,
        },
        "top_paragraphs": ranked[:12],
        "all_paragraphs": paragraph_rows,
        "highlights": highlights,
    }


def _entropy_eval(paragraphs: list[str], tokens: list[str]) -> dict[str, Any]:
    paragraph_rows = []
    for idx, paragraph in enumerate(paragraphs, start=1):
        paragraph_tokens = _tokenize(paragraph)
        entropy = _entropy(paragraph_tokens)
        vocab_size = len(set(paragraph_tokens))
        max_entropy = math.log2(vocab_size) if vocab_size > 1 else 0.0
        normalized = (entropy / max_entropy) if max_entropy > 0 else 0.0
        paragraph_rows.append(
            {
                "paragraph_id": f"p-{idx:04d}",
                "token_count": len(paragraph_tokens),
                "entropy": entropy,
                "normalized_entropy": round(normalized, 4),
            }
        )
    ranked = [row for row in paragraph_rows if row["token_count"] >= MIN_PARAGRAPH_TOKENS]
    ranked.sort(key=lambda row: row["normalized_entropy"])
    low_variety = ranked[:10]
    highlights = []
    if low_variety:
        highlights.append(
            {
                "kind": "entropy",
                "severity": "low",
                "message": f"Lowest normalized entropy appears at {low_variety[0]['paragraph_id']}.",
                "anchors": {"paragraph_id": low_variety[0]["paragraph_id"]},
            }
        )
    return {
        "summary": {
            "document_entropy": _entropy(tokens),
            "evaluated_paragraph_count": len(ranked),
        },
        "paragraphs": paragraph_rows,
        "lowest_variety_paragraphs": low_variety,
        "highlights": highlights,
    }


def _burstiness(paragraphs: list[str]) -> dict[str, Any]:
    paragraph_tokens = [_content_tokens(_tokenize(paragraph)) for paragraph in paragraphs]
    document_frequency = Counter()
    for tokens in paragraph_tokens:
        document_frequency.update(set(tokens))
    paragraph_count = max(len(paragraphs), 1)
    candidate_tokens = [
        token
        for token, df in document_frequency.items()
        if df >= 4 and df <= int(paragraph_count * 0.35)
    ]

    token_occurrence_rows = []
    for token in candidate_tokens:
        counts = [tokens.count(token) for tokens in paragraph_tokens]
        mean = sum(counts) / len(counts)
        variance = sum((count - mean) ** 2 for count in counts) / len(counts)
        std = math.sqrt(variance)
        peak = max(counts)
        if std == 0 or peak == 0:
            continue
        if peak < 2:
            continue
        z_score = (peak - mean) / std
        if z_score < 2.2:
            continue
        peak_paragraph = counts.index(peak) + 1
        token_occurrence_rows.append(
            {
                "token": token,
                "document_frequency": document_frequency[token],
                "peak_count": peak,
                "peak_paragraph_id": f"p-{peak_paragraph:04d}",
                "z_score": round(z_score, 4),
                "burst_strength": round(z_score * peak, 4),
            }
        )
    token_occurrence_rows = [row for row in token_occurrence_rows if row["burst_strength"] >= 9.0]
    token_occurrence_rows.sort(key=lambda row: row["burst_strength"], reverse=True)
    highlights = []
    if token_occurrence_rows:
        top = token_occurrence_rows[0]
        highlights.append(
            {
                "kind": "burstiness",
                "severity": "low",
                "message": f"Burst token '{top['token']}' in {top['peak_paragraph_id']}.",
                "anchors": {"paragraph_id": top["peak_paragraph_id"]},
            }
        )
    return {
        "summary": {"burst_token_count": len(token_occurrence_rows)},
        "bursts": token_occurrence_rows[:25],
        "highlights": highlights,
    }


def _themes(paragraphs: list[str], tokens: list[str]) -> dict[str, Any]:
    paragraph_tokens = [_content_tokens(_tokenize(paragraph)) for paragraph in paragraphs]
    paragraph_count = max(len(paragraphs), 1)
    unigram_counts = Counter()
    unigram_df = Counter()
    for ptokens in paragraph_tokens:
        filtered = [t for t in ptokens if t not in THEME_BLOCKLIST and len(t) >= 4]
        unigram_counts.update(filtered)
        unigram_df.update(set(filtered))
    phrase_counts = Counter()
    phrase_spread: dict[str, set[int]] = defaultdict(set)
    for idx, ptokens in enumerate(paragraph_tokens, start=1):
        for ngram_size in (2, 3):
            for pos in range(len(ptokens) - ngram_size + 1):
                phrase_tokens = ptokens[pos: pos + ngram_size]
                if any(token in THEME_BLOCKLIST for token in phrase_tokens):
                    continue
                phrase = " ".join(phrase_tokens)
                phrase_counts[phrase] += 1
                phrase_spread[phrase].add(idx)

    rows = []
    for token, count in unigram_counts.items():
        df = unigram_df[token]
        if df < 3 or df > int(paragraph_count * 0.35):
            continue
        score = count * math.log1p(paragraph_count / df)
        rows.append({"theme": token, "count": count, "paragraph_spread": df, "type": "token", "score": round(score, 4)})
    for phrase, count in phrase_counts.items():
        if count < 2:
            continue
        spread = len(phrase_spread[phrase])
        if spread < 2:
            continue
        score = count * math.log1p(paragraph_count / spread) * 1.35
        rows.append(
            {
                "theme": phrase,
                "count": count,
                "paragraph_spread": spread,
                "type": "phrase",
                "score": round(score, 4),
            }
        )
    rows.sort(key=lambda row: row["score"], reverse=True)
    top = rows[:20]

    highlights = []
    if top:
        highlights.append(
            {
                "kind": "themes",
                "severity": "info",
                "message": f"Top recurring theme marker: {top[0]['theme']}.",
            }
        )
    return {
        "summary": {"theme_count": len(top)},
        "themes": top,
        "highlights": highlights,
    }


def _patterns(paragraphs: list[str]) -> dict[str, Any]:
    paragraph_tokens = [_tokenize(paragraph) for paragraph in paragraphs]
    bigrams = Counter()
    trigrams = Counter()
    for tokens in paragraph_tokens:
        for idx in range(len(tokens) - 1):
            gram = tokens[idx: idx + 2]
            if any(token in CONTRACTION_PARTS for token in gram):
                continue
            if all(token in STOPWORDS for token in gram):
                continue
            if max(len(token) for token in gram) < 4:
                continue
            bigrams[" ".join(gram)] += 1
        for idx in range(len(tokens) - 2):
            gram = tokens[idx: idx + 3]
            if any(token in CONTRACTION_PARTS for token in gram):
                continue
            if all(token in STOPWORDS for token in gram):
                continue
            if max(len(token) for token in gram) < 4:
                continue
            trigrams[" ".join(gram)] += 1

    top_bigrams = [{"pattern": gram, "count": count} for gram, count in bigrams.most_common(20) if count >= 2]
    top_trigrams = [{"pattern": gram, "count": count} for gram, count in trigrams.most_common(20) if count >= 2]
    highlights = []
    if top_trigrams:
        highlights.append(
            {
                "kind": "patterns",
                "severity": "low",
                "message": f"Most repeated long pattern: '{top_trigrams[0]['pattern']}' ({top_trigrams[0]['count']}x).",
            }
        )
    return {
        "summary": {
            "repeated_bigram_count": len(top_bigrams),
            "repeated_trigram_count": len(top_trigrams),
        },
        "top_bigrams": top_bigrams,
        "top_trigrams": top_trigrams,
        "highlights": highlights,
    }


def run_whole(ctx) -> None:
    normalized_payload = read_json(
        ctx,
        "preprocessed/manuscript_normalized.json",
        family="preprocessed_normalized",
    )
    normalized_text = str(normalized_payload.get("text", ""))
    token_payload = read_json(
        ctx,
        "preprocessed/manuscript_tokens.json",
        family="manuscript_tokens",
    )
    index_payload = read_json(
        ctx,
        "preprocessed/paragraph_index.json",
        family="paragraph_index",
    )

    paragraphs = [row.get("text", "") for row in index_payload.get("paragraphs", []) if isinstance(row, dict)]
    if not paragraphs:
        paragraphs = [part.strip() for part in normalized_text.split("\n\n") if part.strip()]

    raw_tokens = [token for token in token_payload.get("tokens", []) if isinstance(token, str)]
    tokens = [_normalize_token(token) for token in raw_tokens]
    tokens = [token for token in tokens if len(token) >= 2 and token not in CONTRACTION_PARTS]

    jobs = {
        "semantic_repetition": lambda: _semantic_repetition(paragraphs),
        "signal_density": lambda: _signal_density(paragraphs),
        "surprisal": lambda: _surprisal(paragraphs, tokens),
        "entropy": lambda: _entropy_eval(paragraphs, tokens),
        "burstiness": lambda: _burstiness(paragraphs),
        "themes": lambda: _themes(paragraphs, tokens),
        "patterns": lambda: _patterns(paragraphs),
    }
    results: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=min(len(jobs), 6)) as pool:
        future_map = {pool.submit(func): name for name, func in jobs.items()}
        for future, name in ((future, future_map[future]) for future in future_map):
            results[name] = future.result()

    for family, payload in results.items():
        write_json_artifact(ctx, f"diagnostics/{family}.json", payload, family=family)
