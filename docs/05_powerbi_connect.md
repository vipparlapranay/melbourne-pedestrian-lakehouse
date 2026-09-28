# Connecting Power BI to the gold layer (Day 18)

You already know Power BI from Project 1. This is the same skill pointed at a live
warehouse instead of CSV files, which is how it works in a real job.

## Connect

1. In Databricks: **SQL Warehouses → your warehouse → Connection details**. Copy the
   **Server hostname** and **HTTP path**.
2. Generate a token: **User settings → Developer → Access tokens → Generate new token**.
3. In Power BI Desktop: **Get data → Azure Databricks** (or "Databricks"). Paste the
   hostname and HTTP path. Choose **Import** for a small dataset, **DirectQuery** if you
   want the report to always show current data.
4. Authenticate with **Personal Access Token**.
5. Select the `melb.gold` schema and load the four tables.

## Build one page

- Cards: total pedestrians, number of active sensors, days of data, latest reading date
- Line chart: `daily_location_counts` — total by date
- Bar chart: top 10 sensors by total count
- Line chart: `hourly_profile` — average count by hour for a selected sensor
- Table: `weekday_vs_weekend` with the Commuter / Balanced / Destination profile
- Slicer: sensor name

## Worth saying in an interview

"The dashboard reads curated gold tables from the warehouse rather than raw files, so the
business logic lives in one tested place instead of being reimplemented in DAX. Import
mode for speed, DirectQuery when freshness matters more than response time."

## Import vs DirectQuery, briefly

**Import** copies data into the report: fast, works offline, needs a refresh schedule.
**DirectQuery** queries the warehouse on every interaction: always current, slower, and it
keeps the warehouse running (which costs money and quota). For this project, Import is the
sensible default.
