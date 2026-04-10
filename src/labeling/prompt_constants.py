"""Shared constants for compact LLM labeling prompts."""

from __future__ import annotations

PROMPT_SYSTEM = "Classify persona labels from evidence. Prefer broad useful labels. JSON only."

SOURCE_GROUP_HINTS = {
    "reddit": "reddit",
    "stackoverflow": "stackoverflow",
    "shopify_community": "business_communities",
    "hubspot_community": "business_communities",
    "klaviyo_community": "business_communities",
    "google_ads_community": "business_communities",
    "merchant_center_community": "business_communities",
}
