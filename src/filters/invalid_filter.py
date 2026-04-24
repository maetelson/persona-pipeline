"""Validity filtering for normalized posts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(slots=True)
class FilterEvaluation:
    """Evaluation result for one normalized row."""

    invalid_reason: str
    business_signal_score: int
    pain_signal_score: int
    business_signal_terms: str
    pain_signal_terms: str


def _match_keywords(text: str, keywords: list[str]) -> list[str]:
    """Return keywords that appear in the text."""
    lowered = text.lower()
    return [keyword for keyword in keywords if keyword.lower() in lowered]


def _keywords_for_row(rules: dict[str, Any], row: pd.Series, key: str) -> list[str]:
    """Return global plus optional source-specific keywords for one row."""
    keywords = [str(item) for item in rules.get(key, []) or []]
    source_id = str(row.get("source", "") or "")
    overrides = rules.get("source_signal_overrides", {}) or {}
    source_overrides = overrides.get(source_id, {}) if isinstance(overrides, dict) else {}
    extra_keywords = [str(item) for item in source_overrides.get(key, []) or []]
    return list(dict.fromkeys([*keywords, *extra_keywords]))


def _apply_source_reason_exemptions(
    reasons: list[str],
    rules: dict[str, Any],
    row: pd.Series,
    combined_text: str,
    business_hits: list[str],
    pain_hits: list[str],
) -> list[str]:
    """Remove source-specific false-positive invalid reasons when operational context exists."""
    source_id = str(row.get("source", "") or "")
    overrides = rules.get("source_invalid_reason_exemptions", {}) or {}
    source_overrides = overrides.get(source_id, {}) if isinstance(overrides, dict) else {}
    if not isinstance(source_overrides, dict) or not source_overrides:
        return reasons

    updated = list(reasons)
    for reason in list(updated):
        rule_row = source_overrides.get(reason, {})
        if not isinstance(rule_row, dict):
            continue
        when_any_terms = [str(term).lower() for term in rule_row.get("when_any_terms", []) or [] if str(term).strip()]
        require_signal_match = bool(rule_row.get("require_signal_match", True))
        if when_any_terms and not any(term in combined_text for term in when_any_terms):
            continue
        if require_signal_match and not (business_hits or pain_hits):
            continue
        updated.remove(reason)
    return updated


def _apply_source_signal_rescue(
    row: pd.Series,
    combined_text: str,
    business_hits: list[str],
    pain_hits: list[str],
) -> tuple[list[str], list[str]]:
    """Add source-native signal hits when domain terms imply real analytics pain."""
    source_id = str(row.get("source", "") or "")
    rescued_business_hits = list(business_hits)
    rescued_pain_hits = list(pain_hits)

    if source_id == "adobe_analytics_community":
        adobe_metric_terms = [
            "adobe analytics",
            "analysis workspace",
            "workspace",
            "debugger",
            "report suite",
            "report builder",
            "segment",
            "segments",
            "calculated metric",
            "processing rule",
            "date range",
            "data dictionary",
            "evar",
            "prop",
            "classification",
            "classification set",
            "data feed",
            "data warehouse",
            "cja",
            "customer journey analytics",
            "aep",
            "edge network",
            "web sdk",
            "launch",
            "appmeasurement",
            "sitecatalyst",
            "event forwarding",
            "web vitals",
            "xdm",
            "acdl",
            "mcid",
            "ecid",
            "marketing channel",
            "bot rules",
            "tracking server",
            "cross domain",
            "cookie consent",
            "page view",
            "time on site",
            "unique visitor",
            "unique visitors",
            "visits",
            "click thru rate",
            "click through rate",
            "tracking code",
            "short links",
            "mobile device dimension",
            "browser height",
            "browser width",
            "rolling date",
            "text component",
            "internal traffic",
            "livestream api",
            "confidence level",
            "confidence levels",
            "significance",
            "new visitor",
            "new visitors",
            "hour of day",
            "data view",
            "shared with me",
            "ai summary",
            "page summary",
            "freeform table",
            "fallout",
            "entry page",
            "data insertion api",
            "a4t",
            "iab bot list",
            "server call",
            "datastream",
            "library build",
            "activity map",
            "visitor_id",
            "visid_high",
            "visid_low",
            "direct traffic",
            "seo",
            "anomaly alert",
            "customers metric",
        ]
        adobe_pain_terms = [
            "not in workspace",
            "does not appear",
            "doesn't appear",
            "visible in debugger",
            "spike in direct traffic",
            "drop in seo",
            "time delay",
            "not showing up",
            "not showing",
            "not updating",
            "not available",
            "not useful",
            "not provisioned",
            "not reliable",
            "different numbers",
            "higher than",
            "unspecified",
            "seems wrong",
            "double check",
            "not populating",
            "not populated",
            "not adding up",
            "incorrect",
            "impossible",
            "deprecated",
            "deprecation",
            "not firing",
            "not tracking",
            "not resolving",
            "does not tally",
            "not tally",
            "unable to save",
            "out of scale",
            "sent at 00.00",
            "sent after 14.00",
            "way earlier",
            "impact ios",
            "update to ios",
            "very different",
            "not enabled for",
            "can't find",
            "cannot find",
            "aren't matching",
            "not visible",
            "not visible in",
            "not supported",
            "blank rows",
            "two different results",
            "does not load",
            "cannot save request",
        ]
        adobe_question_trouble_terms = [
            "what happened",
            "how come",
            "how does",
            "how can",
            "how do we",
            "how do i confirm",
            "why is",
            "why does",
            "is that expected",
            "is there a way",
            "is it possible",
            "i am still not sure",
            "not sure",
            "less clear",
            "need to understand",
            "need to confirm",
            "need to recreate",
            "used to",
            "no longer",
            "wondering if",
            "can only",
            "doesn't work",
            "doesnt work",
            "different",
            "dependent data block",
            "what is the maximum",
            "greyed out",
            "unexpected",
            "strange",
            "more than",
            "does not make sense",
            "doesnt make sense",
            "not returning all",
            "showing empty",
            "incorrect unique value",
            "what happens after",
            "what do you mean",
            "wondering if anyone knows",
            "surprised to see",
            "perplexed",
            "another dimension",
            "performance optimization",
            "impacts of",
            "is there another",
            "is there any way to remove",
            "best way out",
            "filter out traffic",
            "rolling date labels",
            "dynamic values",
            "migration options",
            "tracking support",
            "look at all the values",
            "what is the right way",
            "how do i read",
            "trying to figure out how to track",
            "does anyone know if there's a way",
            "why i can't find",
            "what does unspecified mean",
            "is this a known bug",
            "when will this be available",
            "how long should i expect to wait",
            "doesn't work in cja",
            "does not work in cja",
            "can't get it to work",
            "cannot get it to work",
            "what is the difference",
            "what's the difference",
            "is there any api available",
            "can this be done natively",
            "not recently",
            "turn off comments",
            "opening frequently by accident",
            "ability to",
            "multiple days of week",
            "upcoming 3 months",
            "upcoming 12 months",
        ]
        adobe_learning_or_noise_terms = [
            "how to implement",
            "css selector",
            "sql query",
            "community member of the year",
            "review adobe analytics on g2",
            "trustradius",
            "guide me or provide a series of steps",
            "example data feed output file",
            "can we create marathon dashboard",
            "best practice to map",
            "backend javascript",
            "server application",
            "how would this be implemented",
            "reactjs",
            "smart tv",
            "integrate and implement",
            "community member of the year",
            "celebrating ",
            "feature suggestion",
            "product capability suggestion",
            "can we create marathon dashboard",
        ]
        if not rescued_business_hits and any(term in combined_text for term in adobe_metric_terms):
            rescued_business_hits.append("adobe_analytics_domain_context")
        has_metric_context = bool(rescued_business_hits) or any(term in combined_text for term in adobe_metric_terms)
        if not rescued_pain_hits and any(term in combined_text for term in adobe_pain_terms):
            rescued_pain_hits.append("adobe_analytics_domain_pain")
        elif (
            not rescued_pain_hits
            and has_metric_context
            and not any(term in combined_text for term in adobe_learning_or_noise_terms)
            and any(term in combined_text for term in adobe_question_trouble_terms)
        ):
            rescued_pain_hits.append("adobe_analytics_operational_question")

    if source_id == "google_developer_forums":
        google_metric_terms = [
            "looker",
            "looker studio",
            "data studio",
            "lookml",
            "dashboard",
            "scorecard",
            "pivot table",
            "table chart",
            "line chart",
            "bar chart",
            "blend data",
            "blended tables",
            "calculated field",
            "calculated metric",
            "scheduled report",
            "scheduled reports",
            "scheduled email",
            "google sheet",
            "google sheets",
            "ga4",
            "bigquery",
            "postgresql connector",
            "date range",
            "summary row",
            "chart export",
            "histogram",
            "scheduled delivery",
            "email delivery",
            "scorecard",
            "extract data",
            "export data",
            "blended data",
            "community visualization",
            "png",
            "datetimefilter",
            "auto date range",
            "custom date range",
            "responsive layout",
            "responsive grid mode",
            "data view",
            "records per page",
            "pagination",
            "conditional formatting",
            "refresh data",
            "date filter",
            "drill-down",
            "drill down",
            "month-over-month",
            "month over month",
            "mom metrics",
            "time frame control",
            "report variables",
            "signed embedding",
            "linking api",
            "embedding",
            "embed report",
            "embedded report",
            "contains field",
            "first record",
            "last record",
            "field value",
            "weight",
            "gsc",
            "render",
            "save",
            "viewer",
            "email report",
            "owner credentials",
            "owner's credentials",
            "owners credentials",
        ]
        google_pain_terms = [
            "disappeared",
            "don't work properly",
            "doesn't work properly",
            "invalid configuration",
            "configuration error",
            "returning null values",
            "null data",
            "summary row is incorrect",
            "summary row incorrect",
            "not usable",
            "incorrect",
            "wrong",
            "different",
            "no luck",
            "can't find a way",
            "cannot find a way",
            "error",
            "stopped working",
            "not loading",
            "cannot be opened",
            "spinning wheel",
            "missing",
            "limitation",
            "limitation or i missed something",
            "does not hide automatically",
            "formula is invalid",
            "data isn't adding",
            "data are different",
            "delivery failed",
            "delivery issue",
            "failed to send",
            "not received",
            "blank export",
            "empty export",
            "wrong total",
            "totals do not match",
            "widget does not match",
            "not same as",
            "over-counting",
            "overcounting",
            "skipped to custom",
            "auto date range condition",
            "won't compare",
            "cannot compare",
            "can't compare",
            "difference between",
            "too low resolution",
            "low resolution",
            "customer presentations",
            "nothing will save",
            "will not save",
            "won't save",
            "same error",
            "cannot explore",
            "can't explore",
            "miscalculating their width",
            "miscalculate their width",
            "does not span correctly",
            "shows all records",
            "show all records",
            "does not display data beyond",
            "still only able",
            "only showing the first",
            "only shows the first",
            "render incorrectly",
            "renders incorrectly",
            "fails to render",
            "cannot render",
            "can't render",
            "truncates the chart",
            "truncated chart",
            "pagination is ignored",
            "records per page is ignored",
            "conditional formatting does not save",
            "email report not received",
            "email report failed",
            "not refreshed",
            "doesn't refresh",
            "does not refresh",
            "cannot access",
            "can't access",
            "cannot be accessed",
            "can't be accessed",
            "not working",
            "independent of the date filter",
            "render correctly when accessed",
            "render correctly only under",
            "invalid formula",
            "won't display correctly",
            "doesn't display correctly",
            "does not display correctly",
            "default behavior",
            "cannot create calculated field",
            "used to display",
            "can no longer schedule without limit",
            "not independent from the date filter",
            "embed without breaking",
            "embedding issue",
            "drill-down data is wrong",
            "drill down data is wrong",
            "row level condition",
            "month over month compare",
            "previous month compare",
            "won't compare to previous month",
            "doesn't compare to previous month",
            "does not compare to previous month",
        ]
        google_question_trouble_terms = [
            "how can we achieve this",
            "how can this be done",
            "is that a limitation",
            "did i miss something",
            "any suggestions on the fix",
            "could someone reach out",
            "why is this happening",
            "what's the fix",
            "what is the fix",
            "how to have",
            "trying to build",
            "trying to add a new data source",
            "why are these numbers different",
            "which number should i trust",
            "which metric should i trust",
            "is there a workaround",
            "how do i explain this",
            "can i do that in looker directly",
            "can i do that in looker",
            "why is this not accepted",
            "how do i compare",
            "is there a way to compare",
            "everything goes well until",
            "once i add it",
            "i experience a glitch",
            "is this expected behavior",
            "i would expect",
            "when i select the date range",
            "i have seen where",
            "how do i stop it from",
            "why does it only show",
            "why is pagination",
            "why does conditional formatting",
            "why are scheduled reports",
            "why is the chart export",
            "is there a way to embed",
            "how do i embed",
            "why is embedding",
            "how to filter drill-down data",
            "i want one of my chart",
            "how to get value for previous month",
            "how to compare with previous month",
            "how do i compare to previous month",
            "how to filter looker report",
            "how can i create a calculated field",
            "how do i add a row level condition",
        ]
        google_learning_or_noise_terms = [
            "welcome to the looker studio community",
            "resource for asking questions",
            "customer council",
            "roadmap",
            "supports multiple language-specific sdks",
            "api now supports",
            "new and expanded mission",
            "feature request",
            "[feature]",
            "one-of-a-kind opportunity",
            "preferred development environment",
            "release notes",
            "conversational analytics api",
            "gemini in bigquery studio",
            "would like to influence",
            "feature] show content as",
            "feature] ",
            "plans to enable this permission",
        ]
        has_google_metric_context = any(term in combined_text for term in google_metric_terms)
        if not rescued_business_hits and has_google_metric_context:
            rescued_business_hits.append("google_reporting_domain_context")
        if not rescued_pain_hits and any(term in combined_text for term in google_pain_terms):
            rescued_pain_hits.append("google_reporting_domain_pain")
        elif (
            not rescued_pain_hits
            and has_google_metric_context
            and not any(term in combined_text for term in google_learning_or_noise_terms)
            and any(term in combined_text for term in google_question_trouble_terms)
        ):
            rescued_pain_hits.append("google_reporting_operational_question")

    if source_id == "domo_community_forum":
        domo_metric_terms = [
            "domo",
            "card",
            "chart",
            "graph by",
            "filter card",
            "data table",
            "analyzer",
            "beast mode",
            "dataset",
            "hourly chart",
            "time scale",
            "filter view",
            "mega table",
            "flex table",
            "pivot table",
            "table card",
            "line chart",
            "bar chart",
            "multi value card",
            "scorecard",
            "rank & window",
            "magic etl",
            "period over period",
            "qualtrics connector",
            "no data message",
            "scheduled report",
            "scheduled reports",
            "predictive analytics",
            "quartile",
            "quartiles",
            "aggregated report",
        ]
        domo_pain_terms = [
            "forcing",
            "hard to get",
            "default selection",
            "not allow user to clear filter",
            "always have something selected",
            "wrong",
            "broken",
            "is it possible",
            "incorrect total",
            "not being shown",
            "doesn't appear",
            "does not appear",
            "only displays",
            "showing blank",
            "standard deviation",
            "won't appear",
            "terminate on the last completed month",
            "terminate at the end of the current month",
            "over 100%",
            "requires formatting",
            "can't find any alternative",
            "cannot find any alternative",
            "aggregated",
            "shows data individually",
            "only the summary",
            "rows with columns",
            "columns with rows",
            "swapping rows with columns",
            "predictive analytics export",
            "not exporting",
            "can't export",
            "cannot export",
        ]
        domo_question_trouble_terms = [
            "can i add another",
            "is there a way",
            "how can i",
            "do i have to",
            "can we create",
            "how would i",
            "is this the only solution",
            "why won't",
            "why does",
            "what is the only solution",
            "how do i get",
            "how do i export",
            "why are scheduled reports",
            "why is it aggregated",
        ]
        domo_learning_or_noise_terms = [
            "domopalooza",
            "release notes",
            "new feature:",
            "current release notes",
            "i attended my first domopalooza",
            "app framework",
            "react",
            "vue",
            "ddx",
        ]
        has_domo_metric_context = any(term in combined_text for term in domo_metric_terms)
        if not rescued_business_hits and has_domo_metric_context:
            rescued_business_hits.append("domo_domain_context")
        if not rescued_pain_hits and any(term in combined_text for term in domo_pain_terms):
            rescued_pain_hits.append("domo_domain_pain")
        elif (
            not rescued_pain_hits
            and has_domo_metric_context
            and not any(term in combined_text for term in domo_learning_or_noise_terms)
            and any(term in combined_text for term in domo_question_trouble_terms)
        ):
            rescued_pain_hits.append("domo_operational_question")

    return rescued_business_hits, rescued_pain_hits


def _evaluate_row(row: pd.Series, rules: dict[str, Any]) -> FilterEvaluation:
    """Evaluate one normalized row against invalid and signal rules."""
    reasons: list[str] = []
    text_len = int(row.get("text_len", 0) or 0)
    language = str(row.get("language", "") or "").lower()
    title = str(row.get("title", "") or "")
    body = str(row.get("body", "") or "")
    comments_text = str(row.get("comments_text", "") or "")
    raw_text = str(row.get("raw_text", "") or "")
    combined_text = " ".join([title, body, comments_text, raw_text]).lower()

    if text_len < int(rules.get("min_text_len", 0)):
        reasons.append("text_too_short")
    if language in {lang.lower() for lang in rules.get("exclude_languages", [])}:
        reasons.append("excluded_language")
    if _match_keywords(combined_text, rules.get("spam_keywords", [])):
        reasons.append("spam_keyword")

    tutorial_hits = _match_keywords(combined_text, rules.get("tutorial_keywords", []))
    syntax_hits = _match_keywords(combined_text, rules.get("syntax_keywords", []))
    promo_hits = _match_keywords(combined_text, rules.get("promo_keywords", []))
    homework_hits = _match_keywords(combined_text, rules.get("student_homework_keywords", []))
    career_hits = _match_keywords(combined_text, rules.get("career_advice_keywords", []))

    if tutorial_hits:
        reasons.append("tutorial_content")
    if syntax_hits:
        reasons.append("syntax_only_question")
    if promo_hits:
        reasons.append("promotional_content")
    if homework_hits:
        reasons.append("student_homework")
    if career_hits:
        reasons.append("career_advice")

    business_hits = _match_keywords(combined_text, _keywords_for_row(rules, row, "business_signal_keywords"))
    pain_hits = _match_keywords(combined_text, _keywords_for_row(rules, row, "pain_signal_keywords"))
    business_hits, pain_hits = _apply_source_signal_rescue(
        row=row,
        combined_text=combined_text,
        business_hits=business_hits,
        pain_hits=pain_hits,
    )
    mode_name = str(rules.get("_active_mode", "") or "")
    mode_profiles = rules.get("mode_profiles", {}) or {}
    mode = mode_profiles.get(mode_name, {}) if isinstance(mode_profiles, dict) else {}
    require_business_signal = bool(mode.get("require_business_signal", True))
    require_pain_signal = bool(mode.get("require_pain_signal", True))
    require_any_signal = bool(mode.get("require_any_signal", False))

    if require_business_signal and not business_hits:
        reasons.append("missing_business_signal")
    if require_pain_signal and not pain_hits:
        reasons.append("missing_pain_signal")
    if require_any_signal and not business_hits and not pain_hits:
        reasons.append("missing_any_signal")
    reasons = _apply_source_reason_exemptions(
        reasons=reasons,
        rules=rules,
        row=row,
        combined_text=combined_text,
        business_hits=business_hits,
        pain_hits=pain_hits,
    )

    return FilterEvaluation(
        invalid_reason="|".join(dict.fromkeys(reasons)),
        business_signal_score=len(set(business_hits)),
        pain_signal_score=len(set(pain_hits)),
        business_signal_terms="|".join(dict.fromkeys(business_hits)),
        pain_signal_terms="|".join(dict.fromkeys(pain_hits)),
    )


def apply_invalid_filter(df: pd.DataFrame, rules: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split normalized posts into valid and invalid dataframes."""
    if df.empty:
        signal_columns = [
            "business_signal_score",
            "pain_signal_score",
            "business_signal_terms",
            "pain_signal_terms",
        ]
        valid_columns = list(df.columns) + signal_columns
        invalid_columns = list(df.columns) + signal_columns + ["invalid_reason"]
        return pd.DataFrame(columns=valid_columns), pd.DataFrame(columns=invalid_columns)

    result = df.copy()
    evaluations = [_evaluate_row(row, rules) for _, row in result.iterrows()]
    result["invalid_reason"] = [item.invalid_reason for item in evaluations]
    result["business_signal_score"] = [item.business_signal_score for item in evaluations]
    result["pain_signal_score"] = [item.pain_signal_score for item in evaluations]
    result["business_signal_terms"] = [item.business_signal_terms for item in evaluations]
    result["pain_signal_terms"] = [item.pain_signal_terms for item in evaluations]

    invalid_df = result[result["invalid_reason"] != ""].copy()
    valid_df = result[result["invalid_reason"] == ""].drop(columns=["invalid_reason"]).copy()
    return valid_df.reset_index(drop=True), invalid_df.reset_index(drop=True)


def activate_rule_mode(rules: dict[str, Any], mode: str | None = None) -> dict[str, Any]:
    """Attach the chosen filter mode so row evaluation can stay stateless."""
    updated = dict(rules)
    selected_mode = str(mode or rules.get("default_mode", "analysis") or "analysis")
    updated["_active_mode"] = selected_mode
    return updated
