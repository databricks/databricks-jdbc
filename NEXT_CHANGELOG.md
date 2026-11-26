# NEXT CHANGELOG

## [Unreleased]

### Added

### Updated

### Fixed
- Preserve String/Number digits for native scale-less DECIMAL/NUMERIC binds using inferred DECIMAL(p,s), rejecting invalid or unrepresentable values at bind time with SQLState INPUT_VALIDATION_ERROR (1015). With native parameters, bounded BigDecimals now use plain wire text and support negative scales (including setBigDecimal and batches); explicit-scale Number conversion preserves float and large-integer digits before rounding and rejects results exceeding 38 digits after rounding.

- Derive the workspace routing header from numeric all-purpose cluster HTTP paths when no explicit header or `?o=` value is present. Respect caller org-ID headers regardless of casing.

---
*Note: When making changes, please add your change under the appropriate section
with a brief description.*
