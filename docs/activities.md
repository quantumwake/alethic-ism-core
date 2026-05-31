# alethic-ism-core — Activities

Running log of what's built, what's queued, and known limitations. (Hand-maintained.)

## Done

- **State persistence: flatten fan-out fixed.** On finalize/save, a query state
  containing an array (e.g. `{"choices": [{"choice": ...}, ...]}`) now fans out into
  multiple rows when `PersistenceMode.INDIVIDUAL_ROWS` + `FlattenMode.DOT_NOTATION`,
  with dot-notation keys normalized to ddl-safe columns (`choices.choice` →
  `choices_choice`). Previously `pre_state_apply` collapsed the flattened list to its
  first row (`rows[0]`), silently dropping every other element.
  (`model/processor_state.py`: `pre_state_apply` → `dict | List[dict]`,
  `_apply_prepared_query_state` per-row helper, `_should_persist_individual_rows()`.)
- **PersistenceMode / FlattenMode are orthogonal.** `PersistenceMode` sets the
  row-level shape; `FlattenMode` sets how complex values render *within* the tabular
  `INDIVIDUAL_ROWS` family. `pre_state_apply` now dispatches on `PersistenceMode`
  first; `CLOB` / (the disabled) `ARRAY_COLUMNS` bypass `FlattenMode` entirely.
- **FlattenMode now distinguishes JSON_STRING vs NONE.** `JSON_STRING` serializes
  complex values to JSON strings → **text** column; `NONE` keeps them native → **json**
  column. (They were previously identical passthroughs.)
- **PersistenceMode.CLOB implemented.** Collapses the whole query state into a single
  `_result_set` JSON character-blob (text) column, one row; ignores `FlattenMode`.
- **Enum renames** (de-confuse `JSON_COLUMN`/`JSON_STRING`): `json_column` → `clob`,
  `list_columns` → `array_columns`. `INDIVIDUAL_ROWS` kept (actively stored on existing
  states; renaming would orphan JSONB configs).

## Known limitations / queued

- **`PersistenceMode.ARRAY_COLUMNS` is DISABLED.** Commented out in the
  `PersistenceMode` enum (`model/processor_state.py`), its `pre_state_apply` branch,
  and the UI persistence-mode dropdowns (`alethic-ism-ui-enterprise`,
  `alethic-ism-ui`). Reason: "single row, each key = `[values…]`" is a **batch-level
  aggregation** — it folds values *across* multiple records into one row, so it cannot
  be derived from a single `query_state` in the per-entry `pre_state_apply`. Re-enabling
  requires a different integration point that sees the full row set (e.g. the
  sync-store / db batch path) rather than a per-row transform.
- **`CLOB` not wired into a queryable storage shape beyond a single text column.** It
  produces one `_result_set` text column; column naming and text-vs-jsonb storage are a
  sensible default and may want revisiting.

## Notes

- Persistence callers that must handle the new fan-out list return from
  `apply_query_state`: `alethic-ism-db` `append_state_data_direct`,
  `alethic-ism-state-sync-store` `save_state`, and core
  `StatePropagationProviderCore.apply_state` (extend-on-list / append-on-dict).
