from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd


CRIME_DEFAULT = "Crime_Data_from_2020_to_Present.csv"
HOLIDAY_DEFAULT = "Hoja de cálculo sin título - Hoja 1.csv"


def normalize_columns(columns: list[str]) -> list[str]:
	normalized = []
	for c in columns:
		cleaned = str(c).replace("\ufeff", "").strip().strip('"').strip("'")
		normalized.append(cleaned.lower().replace(" ", "_").replace("-", "_"))
	return normalized


def load_holiday_lookup(holiday_csv: Path) -> dict[pd.Timestamp, str]:
	df = pd.read_csv(holiday_csv)
	df.columns = normalize_columns(df.columns.tolist())

	# Some exports come as one quoted column: "DATE, FESTIVE_NAME".
	# If that happens, split the single text column into date and festive_name.
	if len(df.columns) == 1:
		single_col = df.columns[0]
		expanded = (
			df[single_col]
			.astype(str)
			.str.strip()
			.str.strip('"')
			.str.split(",", n=1, expand=True)
		)
		if expanded.shape[1] == 2:
			df = pd.DataFrame(
				{
					"date": expanded[0].astype(str).str.strip(),
					"festive_name": expanded[1].astype(str).str.strip(),
				}
			)
		else:
			raise ValueError("Holiday CSV has an unsupported one-column format")

	if "date" not in df.columns:
		raise ValueError("Holiday CSV must contain a DATE column")

	festive_col = "festive_name" if "festive_name" in df.columns else "festive"
	if festive_col not in df.columns:
		raise ValueError("Holiday CSV must contain FESTIVE_NAME column")

	df["date"] = pd.to_datetime(df["date"], errors="coerce")
	df = df.dropna(subset=["date"])
	df["date_only"] = df["date"].dt.normalize()

	grouped = df.groupby("date_only")[festive_col].apply(
		lambda s: " | ".join(sorted({str(x).strip() for x in s if str(x).strip()}))
	)
	return grouped.to_dict()


def parse_time_occ(series: pd.Series) -> tuple[pd.Series, pd.Series]:
	numeric = pd.to_numeric(series, errors="coerce").fillna(0).astype(int)
	hour = (numeric // 100).clip(0, 23)
	minute = (numeric % 100).clip(0, 59)
	return hour, minute


def parse_crime_datetime(series: pd.Series) -> pd.Series:
	parsed = pd.to_datetime(series, format="%m/%d/%Y %I:%M:%S %p", errors="coerce")
	missing_mask = parsed.isna()
	if missing_mask.any():
		fallback = pd.to_datetime(series[missing_mask], format="%m/%d/%Y", errors="coerce")
		parsed.loc[missing_mask] = fallback
	return parsed


def age_bucket(age: float | int | None) -> str:
	if pd.isna(age):
		return "UNKNOWN"
	age_int = int(age)
	if age_int <= 12:
		return "00-12"
	if age_int <= 17:
		return "13-17"
	if age_int <= 24:
		return "18-24"
	if age_int <= 34:
		return "25-34"
	if age_int <= 44:
		return "35-44"
	if age_int <= 54:
		return "45-54"
	if age_int <= 64:
		return "55-64"
	return "65+"


def build_group_frame(
	group_stats: dict,
	crime_sets: dict,
	columns: list[str],
	date_columns: list[str] | None = None,
) -> pd.DataFrame:
	rows = []
	for key, val in group_stats.items():
		key_tuple = key if isinstance(key, tuple) else (key,)
		record = dict(zip(columns, key_tuple))

		incidents = val["incidents"]
		victim_age_count = val["victim_age_count"]

		record["incidents"] = incidents
		record["unique_crime_codes"] = len(crime_sets.get(key, set()))
		record["pct_with_weapon"] = round((val["with_weapon"] / incidents) * 100, 2) if incidents else 0.0
		record["pct_holiday"] = round((val["holiday"] / incidents) * 100, 2) if incidents else 0.0
		record["avg_victim_age"] = (
			round(val["victim_age_sum"] / victim_age_count, 2) if victim_age_count else None
		)
		rows.append(record)

	frame = pd.DataFrame(rows)
	if frame.empty:
		return frame

	if date_columns:
		for col in date_columns:
			frame[col] = pd.to_datetime(frame[col], errors="coerce")

	sort_cols = columns.copy()
	frame = frame.sort_values(sort_cols).reset_index(drop=True)
	return frame


def run_etl(crime_csv: Path, holiday_csv: Path, output_dir: Path, chunksize: int) -> None:
	output_dir.mkdir(parents=True, exist_ok=True)

	holiday_lookup = load_holiday_lookup(holiday_csv)

	fact_path = output_dir / "fact_crimes_enriched.csv"
	if fact_path.exists():
		fact_path.unlink()

	annual_stats = defaultdict(lambda: defaultdict(float))
	monthly_stats = defaultdict(lambda: defaultdict(float))
	weekly_stats = defaultdict(lambda: defaultdict(float))
	daily_stats = defaultdict(lambda: defaultdict(float))
	hourly_stats = defaultdict(lambda: defaultdict(float))
	q15_stats = defaultdict(lambda: defaultdict(float))

	annual_crimes = defaultdict(set)
	monthly_crimes = defaultdict(set)
	weekly_crimes = defaultdict(set)
	daily_crimes = defaultdict(set)
	hourly_crimes = defaultdict(set)
	q15_crimes = defaultdict(set)

	top_crimes_by_year = Counter()
	sex_counts = Counter()
	status_counts = Counter()
	area_counts = Counter()
	victim_descent_counts = Counter()
	weapon_type_counts = Counter()
	sex_age_counts = Counter()

	usecols = [
		"DR_NO",
		"Date Rptd",
		"DATE OCC",
		"TIME OCC",
		"AREA",
		"AREA NAME",
		"Rpt Dist No",
		"Crm Cd",
		"Crm Cd Desc",
		"Vict Age",
		"Vict Sex",
		"Vict Descent",
		"Premis Cd",
		"Premis Desc",
		"Weapon Used Cd",
		"Weapon Desc",
		"Status",
		"Status Desc",
		"LAT",
		"LON",
	]

	first_chunk = True
	total_rows = 0

	for chunk in pd.read_csv(crime_csv, chunksize=chunksize, usecols=usecols, dtype=str):
		total_rows += len(chunk)
		chunk.columns = normalize_columns(chunk.columns.tolist())

		chunk["date_occ"] = parse_crime_datetime(chunk["date_occ"])
		chunk["date_rptd"] = parse_crime_datetime(chunk["date_rptd"])
		chunk = chunk.dropna(subset=["date_occ"])

		hour, minute = parse_time_occ(chunk["time_occ"])
		chunk["hour"] = hour
		chunk["minute"] = minute

		chunk["date"] = chunk["date_occ"].dt.normalize()
		chunk["year"] = chunk["date_occ"].dt.year.astype(int)
		chunk["month"] = chunk["date_occ"].dt.month.astype(int)
		chunk["month_name"] = chunk["date_occ"].dt.month_name()
		chunk["day_of_month"] = chunk["date_occ"].dt.day.astype(int)
		chunk["day_name"] = chunk["date_occ"].dt.day_name()
		chunk["day_of_week"] = chunk["date_occ"].dt.dayofweek.astype(int)
		chunk["is_weekend"] = chunk["day_of_week"].isin([5, 6])

		iso = chunk["date_occ"].dt.isocalendar()
		chunk["iso_year"] = iso["year"].astype(int)
		chunk["iso_week"] = iso["week"].astype(int)

		chunk["quarter_hour_minute"] = ((chunk["minute"] // 15) * 15).astype(int)
		chunk["time_15m"] = chunk["hour"].astype(str).str.zfill(2) + ":" + chunk[
			"quarter_hour_minute"
		].astype(str).str.zfill(2)

		chunk["occurrence_datetime"] = chunk["date"] + pd.to_timedelta(chunk["hour"], unit="h") + pd.to_timedelta(
			chunk["minute"], unit="m"
		)

		chunk["holiday_name"] = chunk["date"].map(holiday_lookup).fillna("")
		chunk["is_holiday"] = chunk["holiday_name"].ne("")

		chunk["vict_age"] = pd.to_numeric(chunk["vict_age"], errors="coerce")
		chunk.loc[(chunk["vict_age"] < 0) | (chunk["vict_age"] > 120), "vict_age"] = pd.NA
		chunk["age_bucket"] = chunk["vict_age"].apply(age_bucket)
		chunk["lat"] = pd.to_numeric(chunk["lat"], errors="coerce")
		chunk["lon"] = pd.to_numeric(chunk["lon"], errors="coerce")

		for col in ["vict_sex", "vict_descent", "weapon_desc", "status_desc", "area_name", "crm_cd_desc"]:
			chunk[col] = chunk[col].fillna("UNKNOWN").astype(str).str.strip()
			chunk.loc[chunk[col] == "", col] = "UNKNOWN"

		chunk["weapon_used"] = chunk["weapon_desc"].str.upper().ne("UNKNOWN")

		fact_cols = [
			"dr_no",
			"date_rptd",
			"date_occ",
			"occurrence_datetime",
			"year",
			"month",
			"month_name",
			"iso_year",
			"iso_week",
			"day_of_month",
			"day_name",
			"day_of_week",
			"is_weekend",
			"hour",
			"minute",
			"time_15m",
			"area",
			"area_name",
			"rpt_dist_no",
			"crm_cd",
			"crm_cd_desc",
			"vict_age",
			"vict_sex",
			"vict_descent",
			"premis_cd",
			"premis_desc",
			"weapon_used_cd",
			"weapon_desc",
			"status",
			"status_desc",
			"lat",
			"lon",
			"is_holiday",
			"holiday_name",
		]

		chunk[fact_cols].to_csv(
			fact_path,
			mode="w" if first_chunk else "a",
			index=False,
			header=first_chunk,
		)
		first_chunk = False

		def update_group(
			grouped: pd.DataFrame,
			stats_dict: dict,
			crimes_dict: dict,
			key_cols: list[str],
		) -> None:
			for row in grouped.itertuples(index=False):
				key = tuple(getattr(row, c) for c in key_cols)
				if len(key) == 1:
					key = key[0]
				stats_dict[key]["incidents"] += int(row.incidents)
				stats_dict[key]["with_weapon"] += int(row.with_weapon)
				stats_dict[key]["holiday"] += int(row.holiday)
				stats_dict[key]["victim_age_sum"] += float(row.victim_age_sum)
				stats_dict[key]["victim_age_count"] += int(row.victim_age_count)

		annual_group = chunk.groupby(["year"]).agg(
			incidents=("dr_no", "count"),
			with_weapon=("weapon_used", "sum"),
			holiday=("is_holiday", "sum"),
			victim_age_sum=("vict_age", "sum"),
			victim_age_count=("vict_age", "count"),
		)
		annual_group = annual_group.reset_index()
		update_group(annual_group, annual_stats, annual_crimes, ["year"])

		monthly_group = chunk.groupby(["year", "month"]).agg(
			incidents=("dr_no", "count"),
			with_weapon=("weapon_used", "sum"),
			holiday=("is_holiday", "sum"),
			victim_age_sum=("vict_age", "sum"),
			victim_age_count=("vict_age", "count"),
		)
		monthly_group = monthly_group.reset_index()
		update_group(monthly_group, monthly_stats, monthly_crimes, ["year", "month"])

		weekly_group = chunk.groupby(["iso_year", "iso_week"]).agg(
			incidents=("dr_no", "count"),
			with_weapon=("weapon_used", "sum"),
			holiday=("is_holiday", "sum"),
			victim_age_sum=("vict_age", "sum"),
			victim_age_count=("vict_age", "count"),
		)
		weekly_group = weekly_group.reset_index()
		update_group(weekly_group, weekly_stats, weekly_crimes, ["iso_year", "iso_week"])

		daily_group = chunk.groupby(["date"]).agg(
			incidents=("dr_no", "count"),
			with_weapon=("weapon_used", "sum"),
			holiday=("is_holiday", "sum"),
			victim_age_sum=("vict_age", "sum"),
			victim_age_count=("vict_age", "count"),
		)
		daily_group = daily_group.reset_index()
		update_group(daily_group, daily_stats, daily_crimes, ["date"])

		hourly_group = chunk.groupby(["hour"]).agg(
			incidents=("dr_no", "count"),
			with_weapon=("weapon_used", "sum"),
			holiday=("is_holiday", "sum"),
			victim_age_sum=("vict_age", "sum"),
			victim_age_count=("vict_age", "count"),
		)
		hourly_group = hourly_group.reset_index()
		update_group(hourly_group, hourly_stats, hourly_crimes, ["hour"])

		q15_group = chunk.groupby(["time_15m"]).agg(
			incidents=("dr_no", "count"),
			with_weapon=("weapon_used", "sum"),
			holiday=("is_holiday", "sum"),
			victim_age_sum=("vict_age", "sum"),
			victim_age_count=("vict_age", "count"),
		)
		q15_group = q15_group.reset_index()
		update_group(q15_group, q15_stats, q15_crimes, ["time_15m"])

		annual_crime_codes = chunk.groupby("year")["crm_cd"].apply(
			lambda s: {str(x).strip() for x in s if pd.notna(x) and str(x).strip()}
		)
		for key, val in annual_crime_codes.items():
			annual_crimes[int(key)].update(val)

		monthly_crime_codes = chunk.groupby(["year", "month"])["crm_cd"].apply(
			lambda s: {str(x).strip() for x in s if pd.notna(x) and str(x).strip()}
		)
		for key, val in monthly_crime_codes.items():
			monthly_crimes[(int(key[0]), int(key[1]))].update(val)

		weekly_crime_codes = chunk.groupby(["iso_year", "iso_week"])["crm_cd"].apply(
			lambda s: {str(x).strip() for x in s if pd.notna(x) and str(x).strip()}
		)
		for key, val in weekly_crime_codes.items():
			weekly_crimes[(int(key[0]), int(key[1]))].update(val)

		daily_crime_codes = chunk.groupby(["date"])["crm_cd"].apply(
			lambda s: {str(x).strip() for x in s if pd.notna(x) and str(x).strip()}
		)
		for key, val in daily_crime_codes.items():
			daily_crimes[pd.Timestamp(key)].update(val)

		hourly_crime_codes = chunk.groupby(["hour"])["crm_cd"].apply(
			lambda s: {str(x).strip() for x in s if pd.notna(x) and str(x).strip()}
		)
		for key, val in hourly_crime_codes.items():
			hourly_crimes[int(key)].update(val)

		q15_crime_codes = chunk.groupby(["time_15m"])["crm_cd"].apply(
			lambda s: {str(x).strip() for x in s if pd.notna(x) and str(x).strip()}
		)
		for key, val in q15_crime_codes.items():
			q15_crimes[str(key)].update(val)

		year_crime_desc = chunk.groupby(["year", "crm_cd_desc"]).size()
		for (year_key, crime_desc), count in year_crime_desc.items():
			top_crimes_by_year[(int(year_key), str(crime_desc))] += int(count)

		sex_counts.update(chunk["vict_sex"].str.upper().fillna("UNKNOWN").tolist())
		status_counts.update(chunk["status_desc"].fillna("UNKNOWN").tolist())
		area_counts.update(chunk["area_name"].fillna("UNKNOWN").tolist())
		victim_descent_counts.update(chunk["vict_descent"].str.upper().fillna("UNKNOWN").tolist())
		weapon_type_counts.update(chunk["weapon_desc"].str.upper().fillna("UNKNOWN").tolist())

		sex_age_series = chunk.groupby([chunk["vict_sex"].str.upper(), "age_bucket"]).size()
		for (sex, bucket), count in sex_age_series.items():
			sex_age_counts[(str(sex), str(bucket))] += int(count)

	annual_df = build_group_frame(annual_stats, annual_crimes, ["year"])
	monthly_df = build_group_frame(monthly_stats, monthly_crimes, ["year", "month"])
	weekly_df = build_group_frame(weekly_stats, weekly_crimes, ["iso_year", "iso_week"])
	daily_df = build_group_frame(daily_stats, daily_crimes, ["date"], date_columns=["date"])
	hourly_df = build_group_frame(hourly_stats, hourly_crimes, ["hour"])
	q15_df = build_group_frame(q15_stats, q15_crimes, ["time_15m"])

	annual_df.to_csv(output_dir / "agg_annual.csv", index=False)
	monthly_df.to_csv(output_dir / "agg_monthly.csv", index=False)
	weekly_df.to_csv(output_dir / "agg_weekly.csv", index=False)
	daily_df.to_csv(output_dir / "agg_daily.csv", index=False)
	hourly_df.to_csv(output_dir / "agg_hourly.csv", index=False)
	q15_df.to_csv(output_dir / "agg_15min.csv", index=False)

	top_rows = [
		{"year": year, "crime_desc": crime_desc, "incidents": count}
		for (year, crime_desc), count in top_crimes_by_year.items()
	]
	top_df = pd.DataFrame(top_rows)
	if not top_df.empty:
		top_df = top_df.sort_values(["year", "incidents"], ascending=[True, False]).reset_index(drop=True)
	top_df.to_csv(output_dir / "kpi_top_crimes_by_year.csv", index=False)

	total_incidents = int(sum(v["incidents"] for v in annual_stats.values()))
	holiday_incidents = int(sum(v["holiday"] for v in annual_stats.values()))
	weapon_incidents = int(sum(v["with_weapon"] for v in annual_stats.values()))

	overview = pd.DataFrame(
		[
			{"metric": "total_incidents", "value": total_incidents},
			{"metric": "holiday_incidents", "value": holiday_incidents},
			{"metric": "holiday_incidents_pct", "value": round((holiday_incidents / total_incidents) * 100, 2) if total_incidents else 0.0},
			{"metric": "incidents_with_weapon", "value": weapon_incidents},
			{"metric": "incidents_with_weapon_pct", "value": round((weapon_incidents / total_incidents) * 100, 2) if total_incidents else 0.0},
			{"metric": "rows_processed", "value": total_rows},
		]
	)
	overview.to_csv(output_dir / "kpi_overview.csv", index=False)

	sex_df = pd.DataFrame([{"vict_sex": k, "incidents": v} for k, v in sex_counts.items()])
	sex_df = sex_df.sort_values("incidents", ascending=False).reset_index(drop=True)
	sex_df.to_csv(output_dir / "kpi_victim_sex_distribution.csv", index=False)

	status_df = pd.DataFrame([{"status_desc": k, "incidents": v} for k, v in status_counts.items()])
	status_df = status_df.sort_values("incidents", ascending=False).reset_index(drop=True)
	status_df.to_csv(output_dir / "kpi_case_status_distribution.csv", index=False)

	area_df = pd.DataFrame([{"area_name": k, "incidents": v} for k, v in area_counts.items()])
	area_df = area_df.sort_values("incidents", ascending=False).reset_index(drop=True)
	area_df.to_csv(output_dir / "kpi_area_distribution.csv", index=False)

	descent_df = pd.DataFrame([{"vict_descent": k, "incidents": v} for k, v in victim_descent_counts.items()])
	descent_df = descent_df.sort_values("incidents", ascending=False).reset_index(drop=True)
	total_descent = descent_df["incidents"].sum() if not descent_df.empty else 0
	descent_df["pct"] = (
		(descent_df["incidents"] / total_descent * 100).round(2) if total_descent else 0
	)
	descent_df.to_csv(output_dir / "kpi_victim_descent_distribution.csv", index=False)

	weapon_df = pd.DataFrame([{"weapon_desc": k, "incidents": v} for k, v in weapon_type_counts.items()])
	weapon_df = weapon_df.sort_values("incidents", ascending=False).reset_index(drop=True)
	weapon_df.to_csv(output_dir / "kpi_weapon_type_distribution.csv", index=False)

	sex_age_df = pd.DataFrame(
		[
			{"vict_sex": sex, "age_bucket": age_group, "incidents": incidents}
			for (sex, age_group), incidents in sex_age_counts.items()
		]
	)
	if not sex_age_df.empty:
		sex_age_df = sex_age_df.sort_values(["vict_sex", "age_bucket", "incidents"], ascending=[True, True, False]).reset_index(drop=True)
	sex_age_df.to_csv(output_dir / "kpi_sex_age_distribution.csv", index=False)

	print(f"ETL finished. Processed rows: {total_rows}")
	print(f"Outputs saved to: {output_dir.resolve()}")


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Clean and enrich LA crime data for BI reporting")
	parser.add_argument("--crime", type=Path, default=Path(CRIME_DEFAULT), help="Path to crime CSV")
	parser.add_argument("--holiday", type=Path, default=Path(HOLIDAY_DEFAULT), help="Path to holiday CSV")
	parser.add_argument("--out", type=Path, default=Path("output"), help="Output folder for consolidated CSVs")
	parser.add_argument("--chunksize", type=int, default=120_000, help="Rows per processing chunk")
	return parser


def main() -> None:
	args = build_parser().parse_args()
	run_etl(args.crime, args.holiday, args.out, args.chunksize)


if __name__ == "__main__":
	main()
