# Dashboard: Traffic Weather Anomaly Detection

Query di bawah aku tulis pakai nama tabel/kolom snake_case standar (`traffic_weather_result`, `pickup_datetime`, dst) berdasarkan label kolom yang muncul di Metabase. **Cek dulu nama tabel & kolom asli** di database `flink_data` (klik gear icon > table metadata di admin panel kalau perlu), terus sesuaikan kalau beda. Syntax di bawah kompatibel PostgreSQL maupun Trino.

---

## 1. Avg Fare by Weather Condition — Bar Chart

```sql
SELECT
  weather_condition,
  ROUND(AVG(fare_amount), 2) AS avg_fare,
  COUNT(*) AS trip_count
FROM traffic_weather_result
GROUP BY weather_condition
ORDER BY avg_fare DESC
```

**Setup di Metabase:**
- New Question → SQL query → paste di atas → Run
- Visualization: **Bar chart**
- X-axis: `weather_condition`, Y-axis: `avg_fare`
- Save sebagai "Avg Fare by Weather"

---

## 2. Precipitation vs Fare/Mile — Scatter Plot (deteksi anomali)

```sql
SELECT
  precipitation,
  fare_amount,
  trip_distance,
  ROUND(fare_amount / NULLIF(trip_distance, 0), 2) AS fare_per_mile,
  pu_location_name,
  weather_condition,
  pickup_datetime
FROM traffic_weather_result
WHERE trip_distance > 0
ORDER BY fare_per_mile DESC
```

**Setup di Metabase:**
- Visualization: **Scatter**
- X-axis: `precipitation`, Y-axis: `fare_per_mile`
- Bubble size (opsional): `fare_amount`
- Titik yang jauh di atas cluster utama = kandidat anomali (fare tinggi relatif ke jarak, saat presipitasi tinggi)
- Save sebagai "Precipitation vs Fare per Mile"

---

## 3. Trip Volume Over Time, by Weather — Stacked Area/Line

```sql
SELECT
  DATE_TRUNC('minute', pickup_datetime) AS pickup_minute,
  weather_condition,
  COUNT(*) AS trip_count
FROM traffic_weather_result
GROUP BY DATE_TRUNC('minute', pickup_datetime), weather_condition
ORDER BY pickup_minute
```

**Setup di Metabase:**
- Visualization: **Line** atau **Area** (pilih "Stacked" di display settings kalau area)
- X-axis: `pickup_minute`
- Y-axis: `trip_count`
- Breakout/series: `weather_condition`
- Save sebagai "Trip Volume by Weather Over Time"
- Kalau demo live, atur granularity ke per-menit atau per-detik sesuai kecepatan stream-nya

---

## 4. Top Pickup Locations saat Cuaca Buruk — Bar Chart

```sql
SELECT
  pu_location_name,
  COUNT(*) AS trip_count,
  ROUND(AVG(fare_amount), 2) AS avg_fare
FROM traffic_weather_result
WHERE weather_condition IN ('storm', 'heavy_rain')
GROUP BY pu_location_name
ORDER BY avg_fare DESC
LIMIT 10
```

**Setup di Metabase:**
- Visualization: **Bar chart** (horizontal biar nama lokasi kebaca)
- X-axis: `pu_location_name`, Y-axis: `avg_fare`
- Save sebagai "Top Locations - Bad Weather Fare Impact"

---

## 5. Anomaly Watchlist — Table

```sql
SELECT
  id,
  pickup_datetime,
  pu_location_name,
  trip_distance,
  fare_amount,
  ROUND(fare_amount / NULLIF(trip_distance, 0), 2) AS fare_per_mile,
  weather_condition,
  precipitation
FROM traffic_weather_result
WHERE trip_distance > 0
  AND (fare_amount / NULLIF(trip_distance, 0)) > 5
ORDER BY fare_per_mile DESC
LIMIT 50
```

> Threshold `> 5` (fare per mile) itu contoh aja — cek dulu distribusi normal fare/mile di data kamu (bisa pakai query `AVG` + `STDDEV`), terus sesuaikan angkanya biar makin akurat nangkep anomali beneran.

**Setup di Metabase:**
- Visualization: **Table**
- Save sebagai "Anomaly Watchlist"

---

## Rakit jadi Dashboard

1. **+ New → Dashboard**, kasih nama "Traffic Weather Anomaly Detection"
2. **+ Add a question**, masukin ke-5 question di atas satu-satu
3. Layout saran:
   - Row 1: "Trip Volume by Weather Over Time" (full width, line/area) — biar keliatan tren real-time
   - Row 2: "Avg Fare by Weather" + "Top Locations - Bad Weather Fare Impact" (dua kolom)
   - Row 3: "Precipitation vs Fare per Mile" (scatter, cukup besar biar titik anomali kebaca)
   - Row 4: "Anomaly Watchlist" (table, full width)
4. **Add a filter** → Date filter (mapping ke `pickup_datetime`) supaya bisa zoom in ke window waktu tertentu pas demo
5. Klik jam icon di kanan atas dashboard → set **auto-refresh** (misal tiap 1 menit) buat efek live streaming pas presentasi hackathon
6. Save
