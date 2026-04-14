# Seed Expansion Plan

## Principles

- Expand source-specific pain phrases, not generic BI head terms.
- Prefer phrases already scored in local diagnostics.
- Add seeds only where the source already converts into useful downstream rows.

## Implemented Seed Additions

### Shopify Community

- `shopify reporting issue`
- `ga4 revenue discrepancy`
- `underreported conversions`
- `underreported sessions`

Why these were chosen:

- They were already scored in local seed diagnostics.
- They stay inside reporting, discrepancy, and trust workflows.
- They should lift both discovery coverage and prefilter survival more safely than broad commerce terms.

### Google Ads Community

- `conversion tracking`
- `remarketing not working`
- `not eligible limited`

Why these were chosen:

- They were the strongest inactive seeds in the local scoring output.
- They stay close to measurement, delivery, and reporting breakdowns.
- They broaden source coverage without turning the source into generic account-support intake.

## Candidate Seeds For Stage 2 Review

### HubSpot Community

- `sales goal revenue`
- `lifecycle stage report`
- `forms filled by month`

Why deferred:

- HubSpot currently uses the API path rather than seed-driven discovery for most of its lift.
- Collection-depth changes should be measured first before adding more seed surface area.

### GitHub Discussions

- Replace zero-yield or near-zero seeds with tighter workflow phrases such as export completeness, source-of-truth review, and metric-formula explanation requests.

Why deferred:

- Current weakness is precision, not lack of raw volume.

### Merchant Center Community

- Focus future additions on `local feed not syncing`, `mismatched product price`, and shipping/data discrepancy phrases.

Why deferred:

- Seed precision must improve before scaling because current prefilter retention is too weak.

## Seeds Explicitly Avoided

- Broad platform names without pain context.
- Generic campaign-management or setup phrases.
- Developer, API, SDK, and implementation-only wording.
- Seeds that mostly drive raw growth into generic support traffic.