# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Exercise rendered X-day SQL without requiring BigQuery credentials."""

import json
from pathlib import Path
import re
import sqlite3
import unittest

import jinja2


ROOT = Path(__file__).resolve().parents[2]
ASSET_SQL = ROOT / "app/assets/bq_queries/asset_performance.sql"
VIDEO_SQL = ROOT / "app/assets/bq_queries/video_campaign_asset_performance.sql"
DISPLAY_SQL = ROOT / "app/assets/bq_queries/display_asset_performance.sql"
AGGREGATE_SQL = ROOT / "app/aggregate/bq_queries/aggregate_performance.sql"
CONFIG_TEMPLATE = ROOT / "app/config.yaml.template"
METRICS = (
    "paying_users", "payment_count", "payment_amount", "retained_users"
)
COLUMNS = [f"{metric}_{day}_day" for day in range(1, 8) for metric in METRICS]
GRAIN = ("date", "network", "ad_group_id", "asset_id", "field_type")
BASE_GRAIN = ("2026-11-01", "SEARCH", 10, 20, "HEADLINE")
SQL_PARTS = re.compile(
    r"'(?:\\.|''|[^'\\])*'|\"(?:\\.|\"\"|[^\"\\])*\""
    r"|`[^`]*`|--[^\n]*|/\*.*?\*/|[(),]",
    re.DOTALL,
)


def render(path, mapping=None, incremental="false"):
    context = {
        "custom_conversions": [],
        "cohort_days": [0, 1, 3, 5, 7, 14, 30],
        "incremental": incremental,
    }
    if mapping is not None:
        context["xday_conversions"] = [mapping] if isinstance(mapping, dict) else mapping
    environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
    return environment.from_string(path.read_text()).render(**context)


def cte_body(sql, name):
    start = re.search(rf"\b{re.escape(name)}\s+AS\s*\(", sql, re.I)
    if start is None:
        raise AssertionError(f"Missing {name} CTE")
    depth = 1
    for token in SQL_PARTS.finditer(sql, start.end()):
        if token.group() == "(":
            depth += 1
        elif token.group() == ")":
            depth -= 1
            if depth == 0:
                return sql[start.end():token.start()]
    raise AssertionError(f"Unclosed {name} CTE")


def sqlite_sql(sql):
    """Adapt identifiers, scalar types, and literals, preserving SQL logic."""
    sql = re.sub(r"`\{bq_dataset\}\.([A-Za-z_]+)`", r"\1", sql)
    sql = re.sub(r"\bAS\s+FLOAT64\b", "AS REAL", sql, flags=re.I)
    sql = re.sub(r"\bAS\s+STRING\b", "AS TEXT", sql, flags=re.I)
    # Jinja tojson emits BigQuery-compatible double-quoted string literals.
    sql = re.sub(
        r'"(?:\\.|[^"\\])*"',
        lambda match: "'" + json.loads(match.group()).replace("'", "''") + "'",
        sql,
    )
    return re.sub(r",\s*(?=FROM\b)", "\n", sql, flags=re.I)


def select_items(projection):
    """Split a projection without splitting CAST/function argument lists."""
    start = 0
    depth = 0
    items = []
    for token in SQL_PARTS.finditer(projection):
        if token.group() == "(":
            depth += 1
        elif token.group() == ")":
            depth -= 1
        elif token.group() == "," and depth == 0:
            items.append(projection[start:token.start()].strip())
            start = token.end()
    if projection[start:].strip():
        items.append(projection[start:].strip())
    return items


def aliased_items(projection):
    projection = SQL_PARTS.sub(
        lambda match: "" if match.group().startswith(("--", "/*")) else match.group(),
        projection,
    )
    items = []
    for item in select_items(projection):
        match = re.search(r"\bAS\s+(\w+)\s*$", item, re.I)
        if match:
            items.append((match.group(1), item))
    return items


class XDayConversionsTest(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.create_function("ConvertAdNetwork", 1, lambda value: value)
        self.db.create_function("ConvertAssetFieldType", 1, lambda value: value)
        self.db.executescript("""
            CREATE TABLE asset_conversion_split (
                date TEXT, network TEXT, ad_group_id INTEGER, asset_id INTEGER,
                field_type TEXT, conversion_id INTEGER, conversions REAL,
                all_conversions REAL, conversions_value REAL,
                all_conversions_value REAL
            );
            CREATE TABLE app_conversions_mapping (
                conversion_id INTEGER, conversion_name TEXT
            );
            CREATE TABLE ap_data (
                date TEXT, network TEXT, ad_group_id INTEGER,
                asset_id INTEGER, field_type TEXT, cost REAL
            );
        """)

    def add_event(
        self, conversion_id, count, value=0, grain=BASE_GRAIN,
        all_count=999, all_value=99999,
    ):
        # Deliberately different all-conversion values catch field mixups.
        self.db.execute(
            "INSERT INTO asset_conversion_split VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (*grain, conversion_id, count, all_count, value, all_value),
        )

    def add_reported_xday_events(self):
        """Create 21 real events; each revenue event supplies two measurements."""
        expected = {}
        for day in range(1, 8):
            events = (
                ("payer", day + 0.25, 12345),
                ("revenue", day * 10 + 0.5, day * 100 + 0.75),
                ("retain", day * 1000 + 0.125, 54321),
            )
            for index, (event, count, value) in enumerate(events):
                conversion_id = day * 10 + index
                self.db.execute(
                    "INSERT INTO app_conversions_mapping VALUES (?, ?)",
                    (conversion_id, f"g_s2s_d{day}_{event}"),
                )
                self.add_event(conversion_id, count, value)
            expected.update({
                f"paying_users_{day}_day": day + 0.25,
                f"payment_count_{day}_day": day * 10 + 0.5,
                f"payment_amount_{day}_day": day * 100 + 0.75,
                f"retained_users_{day}_day": day * 1000 + 0.125,
            })
        self.add_event(9999, 1000000, 1000000)
        return expected

    def evaluate(self, mapping=None):
        sql = render(ASSET_SQL, mapping)
        self.assertNotRegex(sql, r"ASSERT\s+false\b")
        query = (
            f"WITH ConversionMapping AS ({cte_body(sql, 'ConversionMapping')}), "
            f"XDayConversions AS ({cte_body(sql, 'XDayConversions')}) "
            "SELECT * FROM XDayConversions"
        )
        return [dict(row) for row in self.db.execute(sqlite_sql(query))]

    def test_default_mapping_reads_21_reported_events_into_28_independent_metrics(self):
        expected = self.add_reported_xday_events()
        self.db.executemany(
            "INSERT INTO ap_data VALUES (?, ?, ?, ?, ?, ?)",
            [(*BASE_GRAIN, 11), (*BASE_GRAIN, 13)],
        )

        row, = self.evaluate_asset_join(None)

        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM app_conversions_mapping").fetchone()[0],
            21,
        )
        self.assertEqual(row["cost"], 24)
        self.assertEqual({column: row[column] for column in COLUMNS}, expected)

    def test_explicit_config_template_matches_default_events_and_fields(self):
        expected = self.add_reported_xday_events()
        # The checked-in template uses single-line mappings with plain values.
        entries = re.findall(
            r"^\s*-\s*((?:paying_users|payment_count|payment_amount|retained_users)_\d+_day)"
            r":\s*\{([^}]+)\}\s*$",
            CONFIG_TEMPLATE.read_text(),
            re.MULTILINE,
        )
        self.assertEqual(len(entries), 28)
        self.assertEqual({column for column, _ in entries}, set(COLUMNS))
        mapping = []
        event_types = {
            "paying_users": "payer", "payment_count": "revenue",
            "payment_amount": "revenue", "retained_users": "retain",
        }
        for column, fields in entries:
            event = dict(
                (key.strip(), value.strip().strip("\"'"))
                for field in fields.split(",")
                for key, value in [field.split(":", 1)]
            )
            metric, day, _ = column.rsplit("_", 2)
            self.assertEqual(event["conversion_name"], f"g_s2s_d{day}_{event_types[metric]}")
            default_field = "conversions_value" if metric == "payment_amount" else "conversions"
            self.assertEqual(event.get("value_field", default_field), default_field)
            mapping.append({column: event})

        row, = self.evaluate(mapping)

        self.assertEqual({column: row[column] for column in COLUMNS}, expected)

    def evaluate_asset_join(self, mapping):
        """Run actual XDC projections and join at the displayed output grain."""
        sql = render(ASSET_SQL, mapping)
        self.assertNotRegex(sql, r"ASSERT\s+false\b")
        output = re.search(
            r"\n  SELECT\s*\n(.*?)\n  FROM `\{bq_dataset\}\.asset_performance` AS AP",
            sql,
            re.DOTALL,
        )
        join = re.search(
            r"LEFT JOIN XDayConversions AS XDC\s+ON\b.*?(?=\n\s*(?:LEFT JOIN|GROUP BY))",
            sql,
            re.DOTALL,
        )
        self.assertIsNotNone(output)
        self.assertIsNotNone(join)
        output_items = dict(aliased_items(output.group(1)))
        metrics = [
            expression for name, expression in output_items.items()
            if name in COLUMNS
        ]
        self.assertEqual(len(metrics), 28)
        for expression in metrics:
            self.assertRegex(expression, r"AS\s+FLOAT64\)\s+AS\s+\w+$")
        query = (
            f"WITH ConversionMapping AS ({cte_body(sql, 'ConversionMapping')}), "
            f"XDayConversions AS ({cte_body(sql, 'XDayConversions')}) "
            "SELECT AP.date, AP.ad_group_id, AP.asset_id, "
            + output_items["network"] + ", " + output_items["field_type"]
            + ", SUM(AP.cost) AS cost, "
            + ", ".join(metrics)
            + " FROM ap_data AS AP " + join.group()
            + " GROUP BY 1, 2, 3, 4, 5"
        )
        return [dict(row) for row in self.db.execute(sqlite_sql(query))]

    def test_join_preserves_asset_cost_and_includes_assets_without_events(self):
        mapping = {"payment_count_1_day": {"conversion_id": 101}}
        self.add_event(101, 2)
        self.add_event(101, 3)
        for index, alternative in enumerate(
            ("2026-11-02", "DISPLAY", 11, 21, "DESCRIPTION")
        ):
            grain = list(BASE_GRAIN)
            grain[index] = alternative
            self.add_event(101, 1000, grain=tuple(grain))
        self.db.executemany(
            "INSERT INTO ap_data VALUES (?, ?, ?, ?, ?, ?)",
            [
                (*BASE_GRAIN, 10),
                (*BASE_GRAIN, 4),
                (*BASE_GRAIN[:3], 999, BASE_GRAIN[4], 20),
            ],
        )

        rows = {
            row["asset_id"]: row for row in self.evaluate_asset_join(mapping)
        }

        self.assertEqual(set(rows), {20, 999})
        self.assertEqual(rows[20]["cost"], 14)
        self.assertEqual(rows[20]["payment_count_1_day"], 5)
        self.assertEqual(rows[999]["cost"], 20)
        self.assertEqual(rows[999]["payment_count_1_day"], 0)
        self.assertIsNone(rows[999]["retained_users_7_day"])

    def test_normalized_dimensions_combine_events_without_repeating_totals(self):
        self.db.create_function("ConvertAdNetwork", 1, lambda value: "Unknown")
        self.db.create_function("ConvertAssetFieldType", 1, lambda value: "Unknown")
        first = ("2026-11-01", "UNKNOWN_A", 10, 20, "TYPE_A")
        second = ("2026-11-01", "UNKNOWN_B", 10, 20, "TYPE_B")
        self.add_event(101, 2, grain=first)
        self.add_event(101, 3, grain=second)
        self.db.executemany(
            "INSERT INTO ap_data VALUES (?, ?, ?, ?, ?, ?)",
            [(*first, 10), (*second, 20)],
        )

        row, = self.evaluate_asset_join({"payment_count_1_day": {"conversion_id": 101}})

        self.assertEqual(row["network"], "Unknown")
        self.assertEqual(row["field_type"], "Unknown")
        self.assertEqual(row["cost"], 30)
        self.assertEqual(row["payment_count_1_day"], 5)

    def test_multiple_configuration_list_entries_are_merged(self):
        self.add_event(101, 2)
        self.add_event(102, 3)
        mapping = [
            {"paying_users_1_day": {"conversion_id": 101}},
            {"retained_users_7_day": {"conversion_id": 102}},
        ]

        row, = self.evaluate(mapping)

        self.assertEqual(row["paying_users_1_day"], 2)
        self.assertEqual(row["retained_users_7_day"], 3)

    def test_each_reported_day_and_metric_is_independent(self):
        mapping = {}
        expected = {}
        for day in range(1, 8):
            for index, metric in enumerate(METRICS):
                column = f"{metric}_{day}_day"
                event_name = f"reported_{metric}_d{day}"
                conversion_id = day * 10 + index
                count, value = day * 10 + index + 0.25, day * 100 + index + 0.5
                mapping[column] = {"conversion_name": event_name}
                self.db.execute(
                    "INSERT INTO app_conversions_mapping VALUES (?, ?)",
                    (conversion_id, event_name),
                )
                self.add_event(conversion_id, count, value)
                expected[column] = value if metric == "payment_amount" else count
        self.add_event(9999, 1000000, 1000000)

        rows = self.evaluate(mapping)

        self.assertEqual(len(rows), 1)
        self.assertEqual({column: rows[0][column] for column in COLUMNS}, expected)

    def test_reported_people_are_summed_without_counting_rows_or_rounding(self):
        self.add_event(101, 1.25)
        self.add_event(101, 2.5)
        row, = self.evaluate({
            "paying_users_1_day": {"conversion_id": 101},
            "retained_users_7_day": {"conversion_id": "101"},
        })
        self.assertEqual(row["paying_users_1_day"], 3.75)
        self.assertEqual(row["retained_users_7_day"], 3.75)

    def test_mapping_duplicates_and_aliases_do_not_multiply_metrics(self):
        self.db.executemany(
            "INSERT INTO app_conversions_mapping VALUES (?, ?)",
            [(101, "payer"), (101, "payer"), (101, "old payer"), (202, "other")],
        )
        self.add_event(101, 2.5, 30)
        self.add_event(202, 500, 999)
        row, = self.evaluate({
            "paying_users_1_day": {"conversion_name": "payer"},
            "payment_amount_1_day": {"conversion_id": 101},
        })
        self.assertEqual(row["paying_users_1_day"], 2.5)
        self.assertEqual(row["payment_amount_1_day"], 30)

    def test_value_field_override_uses_the_reported_value(self):
        self.add_event(101, 2, 11.5, all_count=17, all_value=23.5)
        row, = self.evaluate({
            "paying_users_1_day": {
                "conversion_id": 101, "value_field": "conversions"
            },
            "payment_count_1_day": {
                "conversion_id": 101, "value_field": "all_conversions"
            },
            "payment_amount_1_day": {
                "conversion_id": 101, "value_field": "conversions_value"
            },
            "retained_users_1_day": {
                "conversion_id": 101, "value_field": "all_conversions_value"
            },
        })
        self.assertEqual(row["paying_users_1_day"], 2)
        self.assertEqual(row["payment_count_1_day"], 17)
        self.assertEqual(row["payment_amount_1_day"], 11.5)
        self.assertEqual(row["retained_users_1_day"], 23.5)

    def test_all_five_grain_dimensions_remain_separate(self):
        grains = [BASE_GRAIN]
        alternatives = ("2026-11-02", "DISPLAY", 11, 21, "DESCRIPTION")
        for index, alternative in enumerate(alternatives):
            grain = list(BASE_GRAIN)
            grain[index] = alternative
            grains.append(tuple(grain))
        expected = {}
        for index, grain in enumerate(grains, start=1):
            self.add_event(101, index, grain=grain)
            expected[grain] = index
        self.add_event(101, 0.5)
        expected[BASE_GRAIN] += 0.5

        rows = self.evaluate({"payment_count_1_day": {"conversion_id": 101}})

        self.assertEqual(len(rows), len(grains))
        self.assertEqual(
            {tuple(row[key] for key in GRAIN): row["payment_count_1_day"] for row in rows},
            expected,
        )

    def test_unconfigured_is_null_and_configured_without_events_is_zero(self):
        self.add_event(101, None, None)
        self.add_event(999, 50, 100)
        row, = self.evaluate({
            "paying_users_1_day": {"conversion_name": "missing event"},
            "payment_count_1_day": {"conversion_id": 101},
            "payment_amount_1_day": {"conversion_id": 101},
        })
        for column in ("paying_users_1_day", "payment_count_1_day", "payment_amount_1_day"):
            self.assertEqual(row[column], 0)
        for column in set(COLUMNS) - {"paying_users_1_day", "payment_count_1_day", "payment_amount_1_day"}:
            self.assertIsNone(row[column])

    def test_event_name_with_quotes_is_matched_literally(self):
        name = "game's \"D1\" payers\\reported {country}"
        self.db.execute("INSERT INTO app_conversions_mapping VALUES (?, ?)", (101, name))
        self.add_event(101, 7)
        mapping = {"paying_users_1_day": {"conversion_name": name}}
        # gaarf applies Python macro formatting after Jinja expansion.
        formatted = render(ASSET_SQL, mapping).format(
            bq_dataset="input", target_dataset="output", date_iso="20261101"
        )
        self.assertIn("`input.asset_conversion_split`", formatted)
        row, = self.evaluate(mapping)
        self.assertEqual(row["paying_users_1_day"], 7)


class NonAppXDayConversionsTest(unittest.TestCase):
    SOURCES = {
        "video": (
            VIDEO_SQL, "video",
            ("date", "account_id", "campaign_id", "ad_group_id", "ad_id", "video_id"),
            ("2026-11-01", 1, 2, 3, 4, "video-main"),
        ),
        "display": (
            DISPLAY_SQL, "display_campaign_performance",
            ("date", "network", "account_id", "campaign_id", "ad_group_id", "ad_id"),
            ("2026-11-01", "DISPLAY", 1, 2, 3, 4),
        ),
    }

    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.create_function("ConvertAdNetwork", 1, lambda value: value)
        self.db.create_function("IEEE_DIVIDE", 2, lambda left, right: left / right)

        class AnyValue:
            def __init__(self):
                self.value = None

            def step(self, value):
                if self.value is None:
                    self.value = value

            def finalize(self):
                return self.value

        self.db.create_aggregate("ANY_VALUE", 1, AnyValue)
        self.db.executescript("""
            CREATE TABLE ad_group_ad_mapping (
                account_id INTEGER, campaign_id INTEGER, ad_group_id INTEGER,
                ad_id INTEGER, account_name TEXT, campaign_name TEXT,
                ad_group_name TEXT, ad_name TEXT, currency TEXT
            );
            CREATE TABLE ocid_mapping (account_id INTEGER, ocid TEXT);
        """)
        for kind, (_, table, grain, _) in self.SOURCES.items():
            self.db.execute(f"""
                CREATE TABLE {table} (
                    date TEXT, network TEXT, account_id INTEGER, campaign_id INTEGER,
                    ad_group_id INTEGER, ad_id INTEGER, video_id TEXT,
                    campaign_type TEXT, account_name TEXT, campaign_name TEXT,
                    ad_group_name TEXT, ad_name TEXT, video_title TEXT, currency TEXT,
                    clicks REAL, impressions REAL, cost REAL, conversions REAL,
                    conversions_value REAL, view_through_conversions REAL,
                    video_trueview_views REAL, p100_views_rate REAL,
                    p25_views_rate REAL, p50_views_rate REAL, p75_views_rate REAL
                )
            """)
            columns = ", ".join(
                f"{name} {'INTEGER' if name.endswith('_id') and name != 'video_id' else 'TEXT'}"
                for name in grain
            )
            self.db.execute(f"""
                CREATE TABLE {kind}_conversion_split (
                    {columns}, conversion_id INTEGER, conversion_name TEXT,
                    conversions REAL, conversions_value REAL,
                    all_conversions REAL, all_conversions_value REAL
                )
            """)

    def insert(self, table, values):
        columns = ", ".join(values)
        placeholders = ", ".join("?" for _ in values)
        self.db.execute(
            f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", tuple(values.values())
        )

    def add_performance(self, kind, grain=None, **overrides):
        _, table, dimensions, default_grain = self.SOURCES[kind]
        keys = dict(zip(dimensions, grain or default_grain))
        name = ":".join(str(keys[key]) for key in (
            "account_id", "campaign_id", "ad_group_id", "ad_id"
        ))
        values = {
            **keys, "campaign_type": kind.upper(), "account_name": "Account",
            "campaign_name": "Campaign", "ad_group_name": "Group", "ad_name": name,
            "video_title": "Video", "currency": "USD", "clicks": 2,
            "impressions": 100, "cost": 1000000, "conversions": 33,
            "conversions_value": 44, "view_through_conversions": 0,
            "video_trueview_views": 10, "p100_views_rate": 0.1,
            "p25_views_rate": 0.4, "p50_views_rate": 0.3, "p75_views_rate": 0.2,
            **overrides,
        }
        self.insert(table, values)
        if kind == "video":
            self.insert("ad_group_ad_mapping", {
                key: values[key] for key in (
                    "account_id", "campaign_id", "ad_group_id", "ad_id",
                    "account_name", "campaign_name", "ad_group_name", "ad_name", "currency",
                )
            })
        self.insert("ocid_mapping", {
            "account_id": keys["account_id"], "ocid": f"ocid-{keys['account_id']}"
        })

    def add_event(
        self, kind, count, value=0, grain=None, conversion_id=101,
        name="g_s2s_d1_revenue", **overrides,
    ):
        _, _, dimensions, default_grain = self.SOURCES[kind]
        self.insert(f"{kind}_conversion_split", {
            **dict(zip(dimensions, grain or default_grain)),
            "conversion_id": conversion_id, "conversion_name": name,
            "conversions": count, "conversions_value": value,
            "all_conversions": 999, "all_conversions_value": 99999,
            **overrides,
        })

    def evaluate(self, kind, mapping=None, incremental="false"):
        sql = render(self.SOURCES[kind][0], mapping, incremental)
        self.assertNotRegex(sql, r"ASSERT\s+false\b")
        query = re.search(
            r"CREATE OR REPLACE TABLE\s+`[^`]+`\s+AS\s*\((.*)\)\s*;\s*$",
            sql, re.DOTALL | re.IGNORECASE,
        )
        self.assertIsNotNone(query)
        # Execute the complete SELECT: CTEs, dimensional mappings, joins and output.
        return [dict(row) for row in self.db.execute(sqlite_sql(query.group(1)))]

    def test_default_21_events_produce_all_28_metrics_in_both_modes(self):
        for kind in self.SOURCES:
            with self.subTest(kind=kind):
                self.add_performance(kind)
                expected = {}
                for day in range(1, 8):
                    for index, (event, count, value) in enumerate((
                        ("payer", day + 0.25, 12345),
                        ("revenue", day * 10 + 0.5, day * 100 + 0.75),
                        ("retain", day * 1000 + 0.125, 54321),
                    )):
                        self.add_event(
                            kind, count, value, conversion_id=day * 10 + index,
                            name=f"g_s2s_d{day}_{event}",
                        )
                    expected.update({
                        f"paying_users_{day}_day": day + 0.25,
                        f"payment_count_{day}_day": day * 10 + 0.5,
                        f"payment_amount_{day}_day": day * 100 + 0.75,
                        f"retained_users_{day}_day": day * 1000 + 0.125,
                    })
                self.add_event(kind, 1000000, 1000000, name="unrelated")
                for incremental in ("false", "true"):
                    row, = self.evaluate(kind, incremental=incremental)
                    self.assertEqual({column: row[column] for column in COLUMNS}, expected)
                    self.assertEqual(row["cost"], 1)

    def test_each_source_grain_is_isolated_including_video_reuse_across_ads(self):
        for kind, (_, _, dimensions, base) in self.SOURCES.items():
            with self.subTest(kind=kind):
                grains = [base]
                for index, value in enumerate(base):
                    grain = list(base)
                    grain[index] = value + 10 if isinstance(value, int) else value + "-other"
                    grains.append(tuple(grain))
                expected = {}
                for count, grain in enumerate(grains, start=1):
                    self.add_performance(kind, grain)
                    self.add_event(kind, count, grain=grain)
                    expected[grain] = count

                rows = self.evaluate(kind)

                self.assertEqual(len(rows), len(grains))
                self.assertEqual({
                    tuple(row[key] for key in dimensions): row["payment_count_1_day"]
                    for row in rows
                }, expected)
                self.assertEqual(sum(row["cost"] for row in rows), len(grains))
                for row in rows:
                    expected_name = ":".join(str(row[key]) for key in (
                        "account_id", "campaign_id", "ad_group_id", "ad_id"
                    ))
                    self.assertEqual(row["ad_name"], expected_name)

    def test_event_rows_sum_while_duplicate_dimension_mappings_do_not_multiply(self):
        for kind in self.SOURCES:
            with self.subTest(kind=kind):
                self.add_performance(kind)
                self.add_event(kind, 1.25, 10)
                self.add_event(kind, 2.5, 20)
                self.db.execute("INSERT INTO ocid_mapping SELECT * FROM ocid_mapping")
                self.db.execute("INSERT INTO ad_group_ad_mapping SELECT * FROM ad_group_ad_mapping")

                row, = self.evaluate(kind)

                self.assertEqual(row["payment_count_1_day"], 3.75)
                self.assertEqual(row["payment_amount_1_day"], 30)
                self.assertEqual(row["cost"], 1)
                self.assertEqual(row["clicks"], 2)
                self.assertEqual(row["impressions"], 100)

    def test_empty_event_sources_retain_performance_with_zero_metrics(self):
        for kind in self.SOURCES:
            with self.subTest(kind=kind):
                self.add_performance(kind)

                row, = self.evaluate(kind)

                self.assertEqual(row["cost"], 1)
                self.assertTrue(all(row[column] == 0 for column in COLUMNS))

    def test_configuration_overrides_partial_nulls_and_explicit_disable(self):
        name = "custom 'event' \\ {day}"
        mapping = [
            {"paying_users_1_day": {"conversion_id": "101"}},
            {"payment_count_1_day": {"conversion_name": name, "value_field": "all_conversions"}},
            {"payment_amount_1_day": {"conversion_id": 101, "value_field": "conversions_value"}},
            {"retained_users_1_day": {"conversion_name": name, "value_field": "all_conversions_value"}},
            {"paying_users_2_day": {"conversion_name": "missing"}},
        ]
        for kind, (path, _, _, _) in self.SOURCES.items():
            with self.subTest(kind=kind):
                self.add_performance(kind)
                self.add_event(kind, 2, 11.5, name=name, all_conversions=17, all_conversions_value=23.5)
                render(path, mapping).format(
                    bq_dataset="input", target_dataset="output", date_iso="20261101"
                )
                row, = self.evaluate(kind, mapping)
                self.assertEqual([row[column] for column in COLUMNS[:5]], [2, 17, 11.5, 23.5, 0])
                self.assertTrue(all(row[column] is None for column in COLUMNS[5:]))

                disabled, = self.evaluate(kind, [{}])
                self.assertEqual(disabled["cost"], 1)
                self.assertTrue(all(disabled[column] is None for column in COLUMNS))
                self.assertNotRegex(render(path, [{}]), r"\bJOIN\s+XDayConversions\b")

    def test_display_joins_raw_network_before_normalizing_output(self):
        self.db.create_function("ConvertAdNetwork", 1, lambda value: "Unknown")
        base = list(self.SOURCES["display"][3])
        for network, count in (("RAW_A", 2), ("RAW_B", 3)):
            base[1] = network
            self.add_performance("display", tuple(base))
            self.add_event("display", count, grain=tuple(base))

        rows = self.evaluate("display")

        self.assertEqual(len(rows), 2)
        self.assertEqual(sorted(row["payment_count_1_day"] for row in rows), [2, 3])
        self.assertTrue(all(row["network"] == "Unknown" for row in rows))
        self.assertEqual(sum(row["cost"] for row in rows), 2)

    def test_video_output_excludes_display_campaigns(self):
        self.add_performance("video")
        other = (*self.SOURCES["video"][3][:-1], "display-video")
        self.add_performance("video", other, campaign_type="DISPLAY")
        self.add_event("video", 5)
        self.add_event("video", 999, grain=other)

        row, = self.evaluate("video")

        self.assertEqual(row["video_id"], "video-main")
        self.assertEqual(row["payment_count_1_day"], 5)
        self.assertEqual(row["campaign_type"], "VIDEO")

    def test_video_without_ad_mapping_keeps_original_identity_and_metrics(self):
        self.add_performance("video")
        self.add_event("video", 5, 25)
        self.db.execute("DELETE FROM ad_group_ad_mapping")

        row, = self.evaluate("video")

        _, _, dimensions, grain = self.SOURCES["video"]
        self.assertEqual(tuple(row[key] for key in dimensions), grain)
        self.assertEqual(row["payment_count_1_day"], 5)
        self.assertEqual(row["payment_amount_1_day"], 25)
        self.assertEqual(row["cost"], 1)
        self.assertIsNone(row["ad_name"])


class XDayOutputSchemaTest(unittest.TestCase):
    def test_duplicate_output_columns_are_rejected_before_table_creation(self):
        for path in (ASSET_SQL, VIDEO_SQL, DISPLAY_SQL):
            with self.subTest(path=path.name):
                sql = render(path, [
                    {"paying_users_1_day": {"conversion_id": 101}},
                    {"paying_users_1_day": {"conversion_id": 102}},
                ])
                self.assertRegex(
                    sql, r"ASSERT\s+false\s+AS\s+'Duplicate xday_conversions output column'"
                )

    def test_invalid_configuration_is_rejected_for_every_campaign_source(self):
        invalid_mappings = (
            {"payment_count_8_day": {"conversion_id": 101}},
            {"payment_count_1_day": {"conversion_id": 101, "conversion_name": "event"}},
            {"payment_count_1_day": {"conversion_id": 101, "value_field": "cost"}},
        )
        for path in (ASSET_SQL, VIDEO_SQL, DISPLAY_SQL):
            for mapping in invalid_mappings:
                with self.subTest(path=path.name, mapping=mapping):
                    self.assertRegex(render(path, mapping), r"ASSERT\s+false\b")

    def test_explicit_empty_mapping_disables_metrics_without_extra_scan(self):
        sql = render(ASSET_SQL, [{}])
        self.assertNotRegex(sql, r"\bXDayConversions\s+AS\s*\(")
        self.assertNotRegex(sql, r"\bJOIN\s+XDayConversions\b")
        for column in COLUMNS:
            self.assertRegex(
                sql,
                rf"CAST\(NULL\s+AS\s+FLOAT64\)\s+AS\s+{column}\b",
            )

    def test_all_six_aggregate_branches_preserve_column_order_and_types(self):
        for incremental in ("false", "true"):
            with self.subTest(incremental=incremental):
                sql = render(AGGREGATE_SQL, {}, incremental)
                projections = re.findall(
                    r"\nSELECT\s*\n(.*?)(?=\nFROM\b|\nWHERE FALSE\b)",
                    sql,
                    re.DOTALL,
                )
                self.assertEqual(len(projections), 6)
                branches = [aliased_items(projection) for projection in projections]
                names = [name for name, _ in branches[0]]
                self.assertEqual(len(names), len(set(names)))
                self.assertEqual(set(names) & set(COLUMNS), set(COLUMNS))
                for index, branch in enumerate(branches):
                    self.assertEqual([name for name, _ in branch], names)
                    for name, expression in branch:
                        if name not in COLUMNS:
                            continue
                        normalized = re.sub(r"\s+", " ", expression).strip()
                        if index < 5:
                            self.assertIn(normalized, (
                                f"{name} AS {name}",
                                f"CAST({name} AS FLOAT64) AS {name}",
                            ))
                        else:
                            self.assertEqual(normalized, f"CAST(NULL AS FLOAT64) AS {name}")

    def test_incremental_output_uses_matching_daily_source_tables(self):
        mapping = {"payment_count_1_day": {"conversion_id": 101}}
        for incremental in ("false", "true"):
            with self.subTest(incremental=incremental):
                suffix = "_{date_iso}" if incremental == "true" else ""
                for path in (ASSET_SQL, VIDEO_SQL, DISPLAY_SQL):
                    self.assertIn(
                        f"CREATE OR REPLACE TABLE `{{target_dataset}}.{path.stem}{suffix}`",
                        render(path, mapping, incremental),
                    )
                aggregate_sql = render(AGGREGATE_SQL, mapping, incremental)
                for variable, table in (
                    ("target_table", "aggregate_asset_performance"),
                    ("asset_table", "asset_performance"),
                    ("video_table", "video_campaign_asset_performance"),
                    ("display_table", "display_asset_performance"),
                ):
                    self.assertIn(
                        f"DECLARE {variable} STRING DEFAULT '{{target_dataset}}.{table}{suffix}';",
                        aggregate_sql,
                    )

    def test_display_aggregate_preserves_network_and_ad_identity(self):
        projections = re.findall(
            r"\nSELECT\s*\n(.*?)(?=\nFROM\b|\nWHERE FALSE\b)",
            render(AGGREGATE_SQL), re.DOTALL,
        )
        for branch in (2, 4):
            with self.subTest(branch=branch):
                fields = dict(aliased_items(projections[branch]))
                self.assertEqual(fields["network"], "network AS network")
                self.assertEqual(fields["ad_id"], "CAST(ad_id AS STRING) AS ad_id")
                self.assertEqual(fields["ad_name"], "ad_name AS ad_name")

    def test_nonapp_source_queries_preserve_grain_and_campaign_boundaries(self):
        directory = ROOT / "app/assets/google_ads_queries"
        for kind, (_, _, grain, _) in NonAppXDayConversionsTest.SOURCES.items():
            with self.subTest(kind=kind):
                query = (directory / f"{kind}_conversion_split.sql").read_text()
                aliases = set(re.findall(r"\bAS\s+(\w+)", query, re.I))
                self.assertTrue(set(grain) <= aliases)
                self.assertTrue({
                    "conversion_id", "conversion_name", "conversions", "conversions_value",
                    "all_conversions", "all_conversions_value",
                } <= aliases)
                resource = "video" if kind == "video" else "ad_group_ad"
                self.assertRegex(query, rf"\bFROM\s+{resource}\b")
                self.assertRegex(query, rf"campaign\.advertising_channel_type\s*=\s*\"{kind.upper()}\"")
        video_query = (directory / "video.sql").read_text()
        self.assertRegex(video_query, r"customer\.id\s+AS\s+account_id")
        self.assertRegex(video_query, r'campaign\.advertising_channel_type\s*=\s*"VIDEO"')


if __name__ == "__main__":
    unittest.main()
