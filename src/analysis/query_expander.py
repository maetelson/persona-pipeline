"""Exploratory second-pass query expansion from first-pass collected text."""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
import re
from typing import Any

import pandas as pd

from src.utils.io import list_jsonl_files, load_yaml, read_jsonl, read_parquet, write_parquet

STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "been", "being", "but", "by", "can", "could", "did", "do", "does",
    "for", "from", "get", "had", "has", "have", "how", "i", "if", "in", "into", "is", "it", "its", "me", "my",
    "of", "on", "or", "our", "out", "so", "that", "the", "their", "them", "there", "this", "to", "too", "up",
    "us", "was", "we", "what", "when", "why", "with", "you", "your",
}

GENERIC_TERMS = {
    "data", "team", "tool", "issue", "problem", "analysis", "business", "work", "using", "user", "users",
    "question", "help", "need", "trying", "way", "process", "workflow", "project", "thing", "things",
}

NOISY_TOKENS = {
    "href", "https", "http", "html", "quot", "nbsp", "nofollow", "noreferrer", "img", "src", "alt", "class",
    "div", "span", "pre", "code", "strong", "blockquote", "section", "github", "com", "stackoverflow",
}

EXCLUDE_PATTERNS = [
    r"\bsyntax\b",
    r"\btutorial\b",
    r"\bbeginner guide\b",
    r"\bhomework\b",
    r"\bassignment\b",
    r"\binstall issue\b",
    r"\bhello world\b",
    r"\bbasic setup\b",
    r"\bcrash only\b",
    r"\bapi error only\b",
]

TOOL_PATTERNS = {
    "excel": r"\bexcel\b|\bxlsx\b|\bworkbook\b",
    "spreadsheet": r"\bspreadsheet\b|\bgoogle sheets\b|\bsheet\b",
    "dashboard": r"\bdashboard\b|\breport view\b|\bkpi dashboard\b",
    "bi": r"\bbi\b|\bbusiness intelligence\b|\breporting tool\b",
    "sql": r"\bsql\b|\bsql query\b|\bquery layer\b",
    "data warehouse": r"\bdata warehouse\b|\bwarehouse\b|\bsnowflake\b|\bbigquery\b|\bredshift\b",
    "data team": r"\bdata team\b|\banalytics team\b|\bbi team\b|\bdata engineering\b",
    "report": r"\breport\b|\breport deck\b|\bweekly report\b|\bmonthly report\b",
}

PAIN_PATTERNS = {
    "numbers do not match": r"\bnumbers do not match\b|\bmetric mismatch\b|\breport discrepancy\b|\bkpi mismatch\b",
    "dashboard not trusted": r"\bdashboard not trusted\b|\bdo not trust dashboard\b|\bdashboard trust issue\b",
    "validate before reporting": r"\bvalidate before reporting\b|\bvalidate spreadsheet before report\b|\bqa before report\b",
    "manual spreadsheet work": r"\bmanual spreadsheet work\b|\bspreadsheet rework\b|\bcopy paste reporting\b",
    "pivot table": r"\bpivot table\b|\bpivot tables\b",
    "ad hoc reporting": r"\bad hoc reporting\b|\bad hoc request\b|\burgent analysis request\b",
    "segment comparison": r"\bsegment comparison\b|\bcompare by segment\b|\bcompare cohorts\b",
    "denominator confusion": r"\bdenominator confusion\b|\bdenominator mismatch\b|\brate definition confusion\b",
    "stakeholder wants excel": r"\bstakeholders still ask for excel\b|\bstakeholder wants excel\b|\bask for excel\b",
    "root cause analysis": r"\broot cause analysis\b|\bdiagnostic analysis\b|\bwhat caused this\b",
}

REPORTING_PATTERNS = {
    "stakeholder explanation": r"\bexplain to stakeholders\b|\bexplain to leadership\b|\bjustify numbers\b",
    "weekly reporting": r"\bweekly reporting\b|\bweekly review\b|\bweekly business review\b",
    "monthly reporting": r"\bmonthly reporting\b|\bmonthly review\b|\bmonth end reporting\b",
    "validation": r"\bvalidation\b|\bqa validation\b|\bnumber check\b",
    "anomaly triage": r"\banomaly triage\b|\banomaly investigation\b|\bdrop triage\b",
}


def build_query_expansion_outputs(root_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build reviewable term-frequency and second-pass query candidate tables.

    This step is intentionally exploratory. It surfaces useful expressions from
    first-pass raw text so a human can review them before updating query design.
    """
    documents = _load_documents(root_dir)
    candidate_columns = [
        "term",
        "source",
        "count",
        "example_snippet",
        "co_occurring_terms",
        "suggested_axis",
        "suggested_query_examples",
        "recommendation_score",
    ]
    frequency_columns = ["term", "source", "frequency", "distinct_raw_count"]

    if not documents:
        return pd.DataFrame(columns=frequency_columns), pd.DataFrame(columns=candidate_columns)

    taxonomy = load_yaml(root_dir / "config" / "query_seed_taxonomy.yaml")
    query_map = load_yaml(root_dir / "config" / "query_map.yaml")
    known_terms = {str(row.get("query_text", "")).lower() for row in query_map.get("expanded_queries", [])}

    term_frequency: Counter[tuple[str, str]] = Counter()
    distinct_raw_ids: dict[tuple[str, str], set[str]] = defaultdict(set)
    snippets: dict[tuple[str, str], list[str]] = defaultdict(list)
    co_occurring: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)

    for document in documents:
        extracted = _extract_document_terms(document["text"], taxonomy)
        extracted_terms = [item["term"] for item in extracted]
        unique_terms = sorted(set(extracted_terms))
        for item in extracted:
            key = (document["source"], item["term"])
            term_frequency[key] += 1
            distinct_raw_ids[key].add(document["raw_id"])
            if len(snippets[key]) < 3 and item["snippet"]:
                snippets[key].append(item["snippet"])
        for term in unique_terms:
            key = (document["source"], term)
            for other in unique_terms:
                if other != term:
                    co_occurring[key][other] += 1

    frequency_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []

    for (source, term), frequency in sorted(term_frequency.items()):
        raw_count = len(distinct_raw_ids[(source, term)])
        frequency_rows.append(
            {
                "term": term,
                "source": source,
                "frequency": int(frequency),
                "distinct_raw_count": int(raw_count),
            }
        )

        if raw_count < 2:
            continue
        if term.lower() in known_terms:
            continue
        if _should_exclude_term(term):
            continue

        top_co_terms = [value for value, _ in co_occurring[(source, term)].most_common(5)]
        recommendation_score = _recommendation_score(
            term=term,
            frequency=frequency,
            distinct_raw_count=raw_count,
            co_terms=top_co_terms,
            suggested_axis=_suggest_axis(term, taxonomy),
        )
        candidate_rows.append(
            {
                "term": term,
                "source": source,
                "count": int(frequency),
                "example_snippet": " | ".join(snippets[(source, term)][:2]),
                "co_occurring_terms": " | ".join(top_co_terms),
                "suggested_axis": _suggest_axis(term, taxonomy),
                "suggested_query_examples": " | ".join(_build_query_examples(term, top_co_terms)),
                "recommendation_score": round(recommendation_score, 3),
            }
        )

    frequency_df = (
        pd.DataFrame(frequency_rows, columns=frequency_columns)
        .sort_values(["source", "frequency", "distinct_raw_count", "term"], ascending=[True, False, False, True])
        .reset_index(drop=True)
    )
    candidates_df = (
        pd.DataFrame(candidate_rows, columns=candidate_columns)
        .sort_values(["recommendation_score", "count", "source", "term"], ascending=[False, False, True, True])
        .reset_index(drop=True)
    )
    return frequency_df, candidates_df


def save_query_expansion_outputs(root_dir: Path) -> tuple[Path, Path]:
    """Write reviewable query-expansion outputs to parquet."""
    frequency_df, candidates_df = build_query_expansion_outputs(root_dir)
    frequency_path = root_dir / "data" / "analysis" / "query_term_frequency.parquet"
    candidates_path = root_dir / "data" / "analysis" / "query_expansion_candidates.parquet"
    write_parquet(frequency_df, frequency_path)
    write_parquet(candidates_df, candidates_path)
    return frequency_path, candidates_path


def _load_documents(root_dir: Path) -> list[dict[str, str]]:
    """Load first-pass raw JSONL, with normalized fallback."""
    documents: list[dict[str, str]] = []
    raw_root = root_dir / "data" / "raw"
    for source_dir in sorted(path for path in raw_root.iterdir() if path.is_dir()) if raw_root.exists() else []:
        for jsonl_path in list_jsonl_files(source_dir):
            for row in read_jsonl(jsonl_path):
                text = combine_text_fields(row)
                if not text:
                    continue
                documents.append(
                    {
                        "source": str(row.get("source", source_dir.name)),
                        "raw_id": str(row.get("raw_id", "")) or f"{source_dir.name}_{len(documents)+1}",
                        "text": text,
                    }
                )

    if documents:
        return documents

    normalized_df = read_parquet(root_dir / "data" / "normalized" / "normalized_posts.parquet")
    for row in normalized_df.to_dict(orient="records"):
        text = combine_text_fields(row)
        if not text:
            continue
        documents.append(
            {
                "source": str(row.get("source", "")),
                "raw_id": str(row.get("raw_id", "")),
                "text": text,
            }
        )
    return documents


def combine_text_fields(row: dict[str, Any]) -> str:
    """Combine title/body/comments/raw_text into one extraction field."""
    parts = [
        str(row.get("title", "") or ""),
        str(row.get("body", "") or ""),
        str(row.get("comments_text", "") or ""),
        str(row.get("raw_text", "") or ""),
    ]
    return "\n\n".join(part for part in parts if part.strip()).strip()


def _extract_document_terms(text: str, taxonomy: dict[str, Any]) -> list[dict[str, str]]:
    """Extract exploratory candidate expressions from one document."""
    normalized = _normalize_source_text(text)
    results: list[dict[str, str]] = []

    for canonical, variants in _build_taxonomy_term_lookup(taxonomy).items():
        if any(variant in normalized for variant in variants):
            results.append({"term": canonical, "snippet": _snippet_for_match(normalized, canonical)})

    for label, pattern in {**TOOL_PATTERNS, **PAIN_PATTERNS, **REPORTING_PATTERNS}.items():
        match = re.search(pattern, normalized)
        if match:
            results.append({"term": label, "snippet": _snippet_for_match(normalized, match.group(0))})

    for phrase in _extract_candidate_phrases(normalized):
        results.append({"term": phrase, "snippet": _snippet_for_match(normalized, phrase)})
    return results


def _extract_candidate_phrases(text: str) -> list[str]:
    """Extract repeated noun-like phrases useful for second-pass queries."""
    tokens = re.findall(r"[a-zA-Z][a-zA-Z0-9]+", text)
    clean_tokens = [token.lower() for token in tokens]
    phrases: list[str] = []
    for n_size in (2, 3):
        for index in range(len(clean_tokens) - n_size + 1):
            ngram = clean_tokens[index : index + n_size]
            if any(token in STOPWORDS for token in ngram):
                continue
            if any(token in NOISY_TOKENS for token in ngram):
                continue
            if any(len(token) < 3 for token in ngram):
                continue
            phrase = " ".join(ngram)
            if _should_exclude_term(phrase):
                continue
            if all(token in GENERIC_TERMS for token in ngram):
                continue
            if _is_too_generic_phrase(phrase):
                continue
            if not _has_signal_token(ngram):
                continue
            phrases.append(phrase)
    return phrases


def _build_taxonomy_term_lookup(taxonomy: dict[str, Any]) -> dict[str, list[str]]:
    """Build a canonical term lookup across taxonomy axes."""
    lookup: dict[str, list[str]] = {}
    for axis_name in ["roles", "problems", "tools", "work_moments"]:
        for _, entry in (taxonomy.get(axis_name, {}) or {}).items():
            label = str(entry.get("label", "")).strip().lower()
            variants = [label]
            for key in ["synonyms", "question_forms", "practitioner_phrases", "bottleneck_phrases", "tool_variants"]:
                variants.extend(str(value).strip().lower() for value in entry.get(key, []) or [])
            variants = [variant for variant in variants if variant]
            if label:
                lookup[label] = sorted(set(variants))
    return lookup


def _normalize_source_text(text: str) -> str:
    """Strip obvious source-specific noise before extraction."""
    normalized = text.lower()
    normalized = re.sub(r"https?://\S+", " ", normalized)
    normalized = re.sub(r"\[([^\]]+)\]\([^)]+\)", r" \1 ", normalized)
    normalized = re.sub(r"<[^>]+>", " ", normalized)
    normalized = re.sub(r"`{1,3}[^`]+`{1,3}", " ", normalized)
    normalized = re.sub(r"#+", " ", normalized)
    normalized = re.sub(r"[_*~|]", " ", normalized)
    normalized = re.sub(r"&[a-z]+;", " ", normalized)
    normalized = re.sub(r"[^a-z0-9\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _snippet_for_match(text: str, term: str) -> str:
    """Build a short example snippet around the first match."""
    idx = text.find(term)
    if idx < 0:
        return text[:180]
    start = max(idx - 70, 0)
    end = min(idx + len(term) + 110, len(text))
    return text[start:end].strip()


def _should_exclude_term(term: str) -> bool:
    """Exclude tutorial/syntax/setup noise and overly short terms."""
    lowered = term.strip().lower()
    if len(lowered) < 4:
        return True
    if any(re.search(pattern, lowered) for pattern in EXCLUDE_PATTERNS):
        return True
    return False


def _is_too_generic_phrase(phrase: str) -> bool:
    """Drop overly generic phrases that will not help query recall."""
    generic_patterns = [
        r"^data [a-z]+$",
        r"^business [a-z]+$",
        r"^team [a-z]+$",
        r"^issue [a-z]+$",
        r"^comment [a-z]+$",
        r"^need help$",
        r"^looking for$",
        r"^can someone$",
        r"^thanks in advance$",
        r"^please help$",
        r"^basic [a-z]+$",
    ]
    return any(re.search(pattern, phrase) for pattern in generic_patterns)


def _has_signal_token(tokens: list[str]) -> bool:
    """Keep phrases only when they contain real collection signal."""
    signal_tokens = {
        "excel", "spreadsheet", "dashboard", "bi", "sql", "warehouse", "report", "reporting", "validate",
        "validation", "mismatch", "segment", "stakeholder", "leadership", "manual", "pivot", "numbers", "drop",
        "root", "automation", "ad", "hoc", "trust",
    }
    return any(token in signal_tokens for token in tokens)


def _term_type(term: str, taxonomy: dict[str, Any]) -> str:
    """Infer the dominant term type from taxonomy and pattern families."""
    for axis_name in ["roles", "problems", "tools", "work_moments"]:
        for _, entry in (taxonomy.get(axis_name, {}) or {}).items():
            if term == str(entry.get("label", "")).strip().lower():
                return axis_name[:-1] if axis_name.endswith("s") else axis_name
    if term in TOOL_PATTERNS:
        return "tool"
    if term in PAIN_PATTERNS:
        return "problem"
    if term in REPORTING_PATTERNS:
        return "work_moment"
    return "candidate_phrase"


def _suggest_axis(term: str, taxonomy: dict[str, Any]) -> str:
    """Suggest which taxonomy axis the term most likely expands."""
    term_type = _term_type(term, taxonomy)
    if term_type in {"role", "problem", "tool", "work_moment"}:
        return term_type
    if "stakeholder" in term or "report" in term or "validation" in term:
        return "problem"
    return "problem"


def _build_query_examples(term: str, co_terms: list[str]) -> list[str]:
    """Build a few reviewable example queries for a candidate term."""
    examples = [
        f"{term} during weekly reporting",
        f"{term} for stakeholder reporting",
    ]
    for co_term in co_terms[:2]:
        examples.append(f"{term} {co_term}")
    deduped: list[str] = []
    for example in examples:
        if example not in deduped:
            deduped.append(example)
    return deduped[:3]


def _recommendation_score(
    term: str,
    frequency: int,
    distinct_raw_count: int,
    co_terms: list[str],
    suggested_axis: str,
) -> float:
    """Assign a lightweight recommendation score for human review priority."""
    score = 0.0
    score += min(distinct_raw_count / 5.0, 1.5)
    score += min(frequency / 8.0, 1.5)
    score += min(len(co_terms) * 0.15, 0.75)
    if suggested_axis in {"problem", "tool"}:
        score += 0.35
    if any(keyword in term for keyword in ["mismatch", "validate", "stakeholder", "excel", "dashboard", "pivot", "manual"]):
        score += 0.5
    if _should_exclude_term(term):
        score -= 0.75
    return max(score, 0.0)
