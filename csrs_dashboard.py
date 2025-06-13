import pandas as pd
import numpy as np
from datetime import datetime
import json
import os

EXCEL_FILE = 'Export Jira CSRs 1.xlsx'
HTML_FILE = 'dashboard.html'

# =============== DATA PROCESSING ===============

def load_data():
    """
    Loads data from the specified Excel file, excluding hidden rows.
    Standardizes column names by stripping whitespace.
    """
    try:
        # 1. Load the data with pandas
        df = pd.read_excel(EXCEL_FILE)
        df.columns = [col.strip() for col in df.columns]

        # 2. Filter for visible rows using openpyxl
        import openpyxl
        wb = openpyxl.load_workbook(EXCEL_FILE, data_only=True)
        sheet = wb.active  # Or wb['Sheet Name'] if you have a specific sheet

        # Collect Excel row numbers that are visible (not hidden)
        visible_rows = [
            row[0].row for row in sheet.iter_rows()
            if not sheet.row_dimensions[row[0].row].hidden
        ]
        # Adjust for pandas zero-based index (Excel row 1 = header), skip header
        visible_indices = [i - 2 for i in visible_rows if i > 1]

        # 3. Filter the DataFrame to only include visible rows
        df = df.iloc[visible_indices].reset_index(drop=True)
        return df

    except FileNotFoundError:
        print(f"Error: Excel file '{EXCEL_FILE}' not found. Please ensure it's in the same directory.")
        exit()
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        exit()


def preprocess_data(df):
    """
    Preprocesses the DataFrame:
    - Converts specified date columns to datetime objects.
    - Converts specified numeric columns to numeric types, coercing errors.
    - Calculates 'Liberación retrasada por' (delay in days) if not already present.
    """
    # Convert date columns to datetime objects
    date_cols = [
        'Creada',
        'Actualizada',
        'Latest Transition to Listo',
        'Fecha Planificada de Liberación',
        'Fecha Real de Liberación'
    ]
    for col in date_cols:
        if col in df.columns:
            # Using errors='coerce' will turn invalid date formats into NaT (Not a Time)
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Force numeric conversion for duration/delay columns, coerce errors to NaN
    numeric_cols = [
        'Liberación retrasada por',
        'Estado Desarrollo > 30 días',
        'Desarrollo y liberada > 60 Días'
    ]
    for col in numeric_cols:
        if col in df.columns:
            # Using errors='coerce' will turn non-numeric values into NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Calculate 'Liberación retrasada por' if it's missing or all NaN
    # This ensures the metric is always available for analysis
    if 'Liberación retrasada por' not in df.columns or df['Liberación retrasada por'].isnull().all():
        if 'Fecha Real de Liberación' in df.columns and 'Fecha Planificada de Liberación' in df.columns:
            # Calculate the difference in days between release and planned release dates
            df['Liberación retrasada por'] = (df['Fecha Real de Liberación'] - df['Fecha Planificada de Liberación']).dt.days
            # Fill any NaN values with 0 and convert to float for consistency
            df['Liberación retrasada por'] = df['Liberación retrasada por'].astype(float)
        else:
            print("Warning: 'Fecha Real de Liberación' or 'Fecha Planificada de Liberación' columns missing. Cannot calculate 'Liberación retrasada por'.")
            df['Liberación retrasada por'] = np.nan # Ensure column exists even if calculation fails

    return df


def get_metrics(df):
    """
    Calculates various KPIs, grouped data, time series, and identifies outliers
    from the preprocessed DataFrame.
    """
    # --- KPIs ---
    total_csrs = len(df)
    # Safely calculate mean, handling inf/-inf and NaN values
    avg_delay = df['Liberación retrasada por'].replace([np.inf, -np.inf], np.nan).dropna().mean()
    # Calculate percentage of late releases (delay > 0)
    pct_late = (df['Liberación retrasada por'] > 0).mean() * 100 if total_csrs > 0 else 0

    avg_dev_gt30 = df['Estado Desarrollo > 30 días'].replace([np.inf, -np.inf], np.nan).dropna().mean()
    num_dev_gt30 = (df['Estado Desarrollo > 30 días'] > 0).sum()

    avg_devlib_gt60 = df['Desarrollo y liberada > 60 Días'].replace([np.inf, -np.inf], np.nan).dropna().mean()
    num_devlib_gt60 = (df['Desarrollo y liberada > 60 Días'] > 0).sum()

    max_delay = df['Liberación retrasada por'].max()
    valid_min_values = df['Liberación retrasada por'][df['Liberación retrasada por'] > 0].dropna()
    min_delay = valid_min_values.min() if not valid_min_values.empty else 0.0


    # --- Grouped data (value counts for categorical columns) ---
    by_estado = df['Estado'].value_counts().to_dict() if 'Estado' in df.columns else {}
    by_pr = df['Pr'].value_counts().to_dict() if 'Pr' in df.columns else {}
    by_tipo = df['T'].value_counts().to_dict() if 'T' in df.columns else {}
    by_persona = df['Persona asignada'].value_counts().to_dict() if 'Persona asignada' in df.columns else {}
    by_dev = df['Desarrollador'].value_counts().to_dict() if 'Desarrollador' in df.columns else {}

    # Delays per person/dev
    # Using .fillna(0) before mean to include cases where there are no delays for a person/dev
    delay_by_persona = df.groupby('Persona asignada')['Liberación retrasada por'].mean().sort_values(ascending=False).to_dict() if 'Persona asignada' in df.columns else {}
    delay_by_dev = df.groupby('Desarrollador')['Liberación retrasada por'].mean().sort_values(ascending=False).to_dict() if 'Desarrollador' in df.columns else {}


    # --- Time series ---
    # Ensure 'Creada' column exists before attempting to create 'Creada_week'
    if 'Creada' in df.columns and not df['Creada'].isnull().all():
        df['Creada_week'] = df['Creada'].dt.to_period('W').astype(str)
        created_trend = df.groupby('Creada_week').size().to_dict()
    else:
        created_trend = {}

    # Ensure 'Fecha Real de Liberación' column exists before attempting to group
    if 'Fecha Real de Liberación' in df.columns and not df['Fecha Real de Liberación'].isnull().all():
        resolved_trend = df.groupby(df['Fecha Real de Liberación'].dt.to_period('W').astype(str)).size().to_dict()
    else:
        resolved_trend = {}

    # --- Outliers (Top 10) ---
    # Ensure columns exist and handle cases where dataframe might be empty or column has all NaNs
    top_late = df.sort_values('Liberación retrasada por', ascending=False).head(10) if 'Liberación retrasada por' in df.columns and not df['Liberación retrasada por'].isnull().all() else pd.DataFrame()
    top_dev_gt30 = df.sort_values('Estado Desarrollo > 30 días', ascending=False).head(10) if 'Estado Desarrollo > 30 días' in df.columns and not df['Estado Desarrollo > 30 días'].isnull().all() else pd.DataFrame()
    top_devlib_gt60 = df.sort_values('Desarrollo y liberada > 60 Días', ascending=False).head(10) if 'Desarrollo y liberada > 60 Días' in df.columns and not df['Desarrollo y liberada > 60 Días'].isnull().all() else pd.DataFrame()

    return dict(
        total_csrs=total_csrs,
        avg_delay=avg_delay if not pd.isna(avg_delay) else 0.0, # Handle NaN for display
        pct_late=pct_late,
        avg_dev_gt30=avg_dev_gt30 if not pd.isna(avg_dev_gt30) else 0.0,
        num_dev_gt30=num_dev_gt30,
        avg_devlib_gt60=avg_devlib_gt60 if not pd.isna(avg_devlib_gt60) else 0.0,
        num_devlib_gt60=num_devlib_gt60,
        max_delay=max_delay if not pd.isna(max_delay) else 0.0,
        min_delay=min_delay if not pd.isna(min_delay) else 0.0,
        by_estado=by_estado,
        by_pr=by_pr,
        by_tipo=by_tipo,
        by_persona=by_persona,
        by_dev=by_dev,
        delay_by_persona=delay_by_persona,
        delay_by_dev=delay_by_dev,
        created_trend=created_trend,
        resolved_trend=resolved_trend,
        top_late=top_late,
        top_dev_gt30=top_dev_gt30,
        top_devlib_gt60=top_devlib_gt60,
    )

# =============== HTML GENERATION ===============

def html_escape(s):
    """Escapes HTML special characters in a string."""
    return str(s).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace("'", '&#039;')

def render_dashboard(df, metrics):
    """
    Renders the HTML dashboard using the calculated metrics.
    Includes sections for KPIs, charts (using Plotly.js), and tables for outliers.
    Adds filter dropdowns and client-side JavaScript for interactivity.
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M')

    # Convert DataFrame to JSON for client-side filtering
    df_json_serializable = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df_json_serializable[col]):
            df_json_serializable[col] = df_json_serializable[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
        elif pd.api.types.is_numeric_dtype(df_json_serializable[col]):
            df_json_serializable[col] = df_json_serializable[col].replace([np.inf, -np.inf], np.nan).fillna(0) # Handle inf and NaN
        elif pd.api.types.is_bool_dtype(df_json_serializable[col]):
            df_json_serializable[col] = df_json_serializable[col].astype(str) # Convert booleans to string
    
    # Ensure all string columns are correctly encoded for JSON and handle NaN
    for col in df_json_serializable.select_dtypes(include='object').columns:
        df_json_serializable[col] = df_json_serializable[col].apply(lambda x: html_escape(x) if pd.notna(x) else '') # Use html_escape here for safety when embedding directly

    # Now, convert to dictionary of records for JSON serialization
    full_data_dict = df_json_serializable.to_dict(orient='records')
    # Use json.dumps for the entire dataset to embed directly into JavaScript
    full_data_json_str = json.dumps(full_data_dict)

    # Extract unique values for filter dropdowns
    filter_options = {
        'Estado': sorted(df['Estado'].dropna().unique().tolist()) if 'Estado' in df.columns else [],
        'Pr': sorted(df['Pr'].dropna().unique().tolist()) if 'Pr' in df.columns else [],
        'T': sorted(df['T'].dropna().unique().tolist()) if 'T' in df.columns else [],
        'Persona asignada': sorted(df['Persona asignada'].dropna().unique().tolist()) if 'Persona asignada' in df.columns else [],
        'Desarrollador': sorted(df['Desarrollador'].dropna().unique().tolist()) if 'Desarrollador' in df.columns else [],
    }

    def generate_options(options_list):
        return ''.join([f'<option value="{html_escape(opt)}">{html_escape(opt)}</option>' for opt in options_list])

    # Get min and max dates for date filter defaults
    min_release_date = df['Fecha Real de Liberación'].min().strftime('%Y-%m-%d') if not df['Fecha Real de Liberación'].isnull().all() else ''
    max_release_date = df['Fecha Real de Liberación'].max().strftime('%Y-%m-%d') if not df['Fecha Real de Liberación'].isnull().all() else ''

    filters_html = f"""
    <div class="section filters-section">
        <h2>Filter Data</h2>
        <div class="filters-grid">
            <div class="filter-group">
                <label for="filterEstado">Estado:</label>
                <select id="filterEstado" onchange="filterData()">
                    <option value="">All</option>
                    {generate_options(filter_options['Estado'])}
                </select>
            </div>
            <div class="filter-group">
                <label for="filterPr">Prioridad:</label>
                <select id="filterPr" onchange="filterData()">
                    <option value="">All</option>
                    {generate_options(filter_options['Pr'])}
                </select>
            </div>
            <div class="filter-group">
                <label for="filterTipo">Tipo:</label>
                <select id="filterTipo" onchange="filterData()">
                    <option value="">All</option>
                    {generate_options(filter_options['T'])}
                </select>
            </div>
            <div class="filter-group">
                <label for="filterPersona">Persona Asignada:</label>
                <select id="filterPersona" onchange="filterData()">
                    <option value="">All</option>
                    {generate_options(filter_options['Persona asignada'])}
                </select>
            </div>
            <div class="filter-group">
                <label for="filterDesarrollador">Desarrollador:</label>
                <select id="filterDesarrollador" onchange="filterData()">
                    <option value="">All</option>
                    {generate_options(filter_options['Desarrollador'])}
                </select>
            </div>
            <div class="filter-group">
                <label for="startDate">Fecha Liberación (Start):</label>
                <input type="date" id="startDate" value="{min_release_date}" onchange="filterData()">
            </div>
            <div class="filter-group">
                <label for="endDate">Fecha Liberación (End):</label>
                <input type="date" id="endDate" value="{max_release_date}" onchange="filterData()">
            </div>
            <button onclick="resetFilters()">Reset Filters</button>
        </div>
    </div>
    """

    # Cards section
    cards_html = f"""
    <div class="cards-row">
      <div class="card"><div class="card-label">Total CSRs</div><div class="card-value" id="kpiTotalCSRs">{metrics['total_csrs']}</div></div>
      <div class="card"><div class="card-label">Avg Delay (Days)</div><div class="card-value" id="kpiAvgDelay">{metrics['avg_delay']:.1f}</div></div>
      <div class="card"><div class="card-label">% Released Late</div><div class="card-value" id="kpiPctLate">{metrics['pct_late']:.1f}%</div></div>
      <div class="card"><div class="card-label">>30d in Dev</div><div class="card-value" id="kpiNumDevGT30">{metrics['num_dev_gt30']}</div></div>
      <div class="card"><div class="card-label">>60d Dev→Release</div><div class="card-value" id="kpiNumDevLibGT60">{metrics['num_devlib_gt60']}</div></div>
      <div class="card"><div class="card-label">Longest Delay (Days)</div><div class="card-value" id="kpiMaxDelay">{metrics['max_delay']:.1f}</div></div>
      <div class="card"><div class="card-label">Shortest Delay (Days)</div><div class="card-value" id="kpiMinDelay">{metrics['min_delay']:.1f}</div></div>
      <div class="card"><div class="card-label">Last updated</div><div class="card-value">{now}</div></div>
    </div>
    """

    # Top delayed CSRs table
    def make_table_html(data_rows, col_names):
        """Generates HTML table rows from a list of dictionaries (from JS-filtered data)."""
        rows_html = []
        for r in data_rows:
            # Safely get values, converting numbers to fixed-point strings where appropriate
            summary = html_escape(r.get('Resumen', ''))
            key = html_escape(r.get('Clave', ''))
            # Get the value for the specific metric column (e.g., 'Liberación retrasada por')
            metric_col_name = col_names[2] # Assumes the 3rd element in col_names is the metric key
            col_value = r.get(metric_col_name, 0)
            if isinstance(col_value, (int, float)):
                col_value = f"{col_value:.1f}" if isinstance(col_value, float) else str(col_value)
            else:
                col_value = str(col_value)
            assignee = html_escape(r.get('Persona asignada', ''))
            developer = html_escape(r.get('Desarrollador', ''))

            rows_html.append(
                f"<tr><td>{summary}</td><td>{key}</td><td>{col_value}</td><td>{assignee}</td><td>{developer}</td></tr>"
            )
        return ''.join(rows_html)

    # Initial table data (using Python-calculated top N)
    table_late = make_table_html(metrics['top_late'].to_dict(orient='records'), ['Resumen', 'Clave', 'Liberación retrasada por', 'Persona asignada', 'Desarrollador'])
    table_dev_gt30 = make_table_html(metrics['top_dev_gt30'].to_dict(orient='records'), ['Resumen', 'Clave', 'Estado Desarrollo > 30 días', 'Persona asignada', 'Desarrollador'])
    table_devlib_gt60 = make_table_html(metrics['top_devlib_gt60'].to_dict(orient='records'), ['Resumen', 'Clave', 'Desarrollo y liberada > 60 Días', 'Persona asignada', 'Desarrollador'])


    # HTML output template
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CSR Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;700;900&display=swap" rel="stylesheet">
    <style>
        body {{
            font-family: 'Inter', sans-serif;
            background: #f5f6fa;
            color: #222;
            margin: 0;
            padding: 0;
            line-height: 1.6;
        }}
        .header {{
            background: linear-gradient(90deg, #6a82fb 0%, #fc5c7d 100%);
            color: #fff;
            padding: 2rem 1rem;
            text-align: center;
            border-bottom-left-radius: 1rem;
            border-bottom-right-radius: 1rem;
            box-shadow: 0 4px 10px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            font-size: 2.8rem;
            margin-bottom: 0.5rem;
            font-weight: 900;
        }}
        .header p {{
            font-size: 1.1rem;
            opacity: 0.9;
        }}
        .cards-row {{
            display: flex;
            flex-wrap: wrap;
            gap: 1.5rem;
            justify-content: center;
            margin: -3rem auto 2rem auto; /* Overlap with header for visual effect */
            max-width: 1200px;
            padding: 0 1rem;
        }}
        .card {{
            background: #fff;
            border-radius: 1rem;
            box-shadow: 0 4px 12px rgba(0,0,0,0.08);
            padding: 1.5rem 2rem;
            text-align: center;
            min-width: 160px;
            flex: 1 1 auto;
            transition: transform 0.2s ease-in-out;
        }}
        .card:hover {{
            transform: translateY(-5px);
        }}
        .card-label {{
            font-size: 0.9rem;
            color: #888;
            margin-bottom: 0.3rem;
            font-weight: 700;
            text-transform: uppercase;
        }}
        .card-value {{
            font-size: 2.5rem;
            font-weight: 900;
            color: #6a82fb;
        }}
        .section {{
            margin: 2rem auto;
            max-width: 1200px;
            background: #fff;
            border-radius: 1rem;
            box-shadow: 0 4px 12px rgba(0,0,0,0.08);
            padding: 2rem;
        }}
        h2 {{
            color: #333;
            margin-bottom: 1.5rem;
            font-size: 1.8rem;
            border-bottom: 2px solid #eee;
            padding-bottom: 0.5rem;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 1rem;
            border-radius: 0.5rem;
            overflow: hidden; /* Ensures rounded corners on table */
        }}
        th, td {{
            border-bottom: 1px solid #eee;
            padding: 0.8rem 1rem;
            font-size: 0.95rem;
            text-align: left;
        }}
        th {{
            color: #fff;
            background: #6a82fb; /* Use a primary color for headers */
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        tr:nth-child(even) {{
            background: #f9f9fc;
        }}
        tr:hover td {{
            background: #eef1f9;
        }}
        .charts-row {{
            display: flex;
            flex-wrap: wrap;
            gap: 2rem;
            justify-content: center;
        }}
        .chart-block {{
            flex: 1 1 45%; /* Adjust for 2 columns on larger screens */
            min-width: 300px; /* Minimum width for charts */
            padding: 1rem;
            box-sizing: border-box; /* Include padding in element's total width */
        }}
        #trendChart {{
            width: 100%; /* Make trend chart span full width */
            min-height: 400px;
        }}
        .filters-section {{
            padding-top: 0;
            margin-top: 1rem;
        }}
        .filters-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            align-items: end;
        }}
        .filter-group label {{
            display: block;
            margin-bottom: 0.5rem;
            font-weight: 700;
            color: #555;
        }}
        .filter-group select, .filter-group input[type="date"] {{
            width: 100%;
            padding: 0.6rem;
            border: 1px solid #ddd;
            border-radius: 0.5rem;
            background-color: #fff;
            font-size: 1rem;
            box-shadow: inset 0 1px 3px rgba(0,0,0,0.05);
        }}
        .filters-grid button {{
            background: #4CAF50;
            color: white;
            padding: 0.8rem 1.5rem;
            border: none;
            border-radius: 0.5rem;
            cursor: pointer;
            font-size: 1rem;
            font-weight: 700;
            transition: background-color 0.3s ease;
        }}
        .filters-grid button:hover {{
            background-color: #45a049;
        }}
        @media (max-width: 900px) {{
            .cards-row {{
                margin-top: 1rem;
                flex-direction: column;
                align-items: center;
            }}
            .card {{
                min-width: 80%; /* Cards take more width on small screens */
            }}
            .charts-row {{
                flex-direction: column;
                align-items: center;
            }}
            .chart-block {{
                min-width: 95%; /* Charts take full width on small screens */
            }}
            .header h1 {{
                font-size: 2rem;
            }}
            .header p {{
                font-size: 0.9rem;
            }}
            .filters-grid {{
                grid-template-columns: 1fr; /* Stack filters on small screens */
            }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>CSR Analytics Dashboard</h1>
        <p>Automated metrics & analysis from Jira export</p>
    </div>
    {cards_html}
    {filters_html}
    <div class="section">
        <h2>Status, Priority & Type Distribution</h2>
        <div class="charts-row">
            <div class="chart-block"><div id="estadoPie"></div></div>
            <div class="chart-block"><div id="prPie"></div></div>
            <div class="chart-block"><div id="tipoPie"></div></div>
        </div>
    </div>
    <div class="section">
        <h2>Trends: CSRs Created & Released by Week</h2>
        <div id="trendChart"></div>
    </div>
    <div class="section">
        <h2>Top 10 Most Delayed Releases</h2>
        <table id="tableLate">
            <thead><tr><th>Resumen</th><th>Clave</th><th>Días retraso</th><th>Persona asignada</th><th>Desarrollador</th></tr></thead>
            <tbody>{table_late}</tbody>
        </table>
    </div>
    <div class="section">
        <h2>Top 10 Longest Development (>30d)</h2>
        <table id="tableDevGT30">
            <thead><tr><th>Resumen</th><th>Clave</th><th>Días en dev</th><th>Persona asignada</th><th>Desarrollador</th></tr></thead>
            <tbody>{table_dev_gt30}</tbody>
        </table>
    </div>
    <div class="section">
        <h2>Top 10 Longest Dev to Release (>60d)</h2>
        <table id="tableDevLibGT60">
            <thead><tr><th>Resumen</th><th>Clave</th><th>Días dev→liberada</th><th>Persona asignada</th><th>Desarrollador</th></tr></thead>
            <tbody>{table_devlib_gt60}</tbody>
        </table>
    </div>
    <div class="section">
        <h2>Workload by Assignee / Developer</h2>
        <div class="charts-row">
            <div class="chart-block"><div id="personaBar"></div></div>
            <div class="chart-block"><div id="devBar"></div></div>
        </div>
    </div>
    <div class="section">
        <h2>Average Delay by Person / Developer</h2>
        <div class="charts-row">
            <div class="chart-block"><div id="personaDelay"></div></div>
            <div class="chart-block"><div id="devDelay"></div></div>
        </div>
    </div>
    <script>
        // Store original data
        // Use JSON.parse to safely parse the JSON string embedded by Python
        const originalData = JSON.parse(`{full_data_json_str}`);
        let currentData = originalData;

        // Function to calculate metrics from a given dataset
        function calculateMetrics(data) {{
            const total_csrs = data.length;

            // Handle potential non-numeric values in 'Liberación retrasada por'
            const delayValues = data.map(d => parseFloat(d['Liberación retrasada por']) || 0);
            const validDelays = delayValues.filter(d => !isNaN(d) && isFinite(d));
            const avg_delay = validDelays.length > 0 ? validDelays.reduce((a, b) => a + b, 0) / validDelays.length : 0;
            const pct_late = total_csrs > 0 ? (delayValues.filter(d => d > 0).length / total_csrs) * 100 : 0;
            const max_delay = validDelays.length > 0 ? Math.max(...validDelays) : 0;
            const min_delay = validDelays.length > 0 ? Math.min(...validDelays) : 0;

            const dev30Values = data.map(d => parseFloat(d['Estado Desarrollo > 30 días']) || 0);
            const validDev30 = dev30Values.filter(d => !isNaN(d) && isFinite(d));
            const avg_dev_gt30 = validDev30.length > 0 ? validDev30.reduce((a, b) => a + b, 0) / validDev30.length : 0;
            const num_dev_gt30 = dev30Values.filter(d => d > 0).length;

            const dev60Values = data.map(d => parseFloat(d['Desarrollo y liberada > 60 Días']) || 0);
            const validDev60 = dev60Values.filter(d => !isNaN(d) && isFinite(d));
            const avg_devlib_gt60 = validDev60.length > 0 ? validDev60.reduce((a, b) => a + b, 0) / validDev60.length : 0;
            const num_devlib_gt60 = dev60Values.filter(d => d > 0).length;

            // Grouped data
            function getCounts(arr, key) {{
                return arr.reduce((acc, obj) => {{
                    const val = obj[key];
                    if (val) acc[val] = (acc[val] || 0) + 1;
                    return acc;
                }}, {{}});
            }}

            function getAvgDelay(arr, key) {{
                const groups = arr.reduce((acc, obj) => {{
                    const val = obj[key];
                    // Ensure 'Liberación retrasada por' is treated as a number
                    const delay = parseFloat(obj['Liberación retrasada por']) || 0;
                    if (val) {{
                        if (!acc[val]) acc[val] = {{ sum: 0, count: 0 }};
                        acc[val].sum += delay;
                        acc[val].count += 1;
                    }}
                    return acc;
                }}, {{}});
                const result = {{}};
                for (const k in groups) {{
                    result[k] = groups[k].sum / groups[k].count;
                }}
                return Object.fromEntries(Object.entries(result).sort(([, a], [, b]) => b - a)); // Sort descending
            }}

            const by_estado = getCounts(data, 'Estado');
            const by_pr = getCounts(data, 'Pr');
            const by_tipo = getCounts(data, 'T');
            const by_persona = getCounts(data, 'Persona asignada');
            const by_dev = getCounts(data, 'Desarrollador');

            const delay_by_persona = getAvgDelay(data, 'Persona asignada');
            const delay_by_dev = getAvgDelay(data, 'Desarrollador');

            // Time series
            function getTrend(arr, dateKey) {{
                const trend = arr.reduce((acc, obj) => {{
                    const dateStr = obj[dateKey];
                    if (dateStr) {{
                        const date = new Date(dateStr);
                        // Get week string (YYYY-WW)
                        const year = date.getFullYear();
                        const firstDayOfYear = new Date(year, 0, 1);
                        // Calculate day of the year (0-indexed)
                        const dayOfYear = (date - firstDayOfYear) / (24 * 60 * 60 * 1000);
                        // Week number (1-indexed, starting from Monday as first day of week)
                        const week = Math.ceil((dayOfYear + firstDayOfYear.getDay() + 1) / 7);
                        const weekStr = `${{year}}-W${{String(week).padStart(2, '0')}}`;
                        acc[weekStr] = (acc[weekStr] || 0) + 1;
                    }}
                    return acc;
                }}, {{}});
                return Object.fromEntries(Object.entries(trend).sort()); // Sort by week string
            }}

            const created_trend = getTrend(data, 'Creada');
            const resolved_trend = getTrend(data, 'Fecha Real de Liberación');

            // Top 10 tables
            function getTop10(arr, sortKey) {{
                if (!sortKey || !arr || arr.length === 0) return [];
                const sorted = [...arr].sort((a, b) => (parseFloat(b[sortKey]) || 0) - (parseFloat(a[sortKey]) || 0));
                return sorted.slice(0, 10).map(d => ({{
                    Resumen: d.Resumen,
                    Clave: d.Clave,
                    [sortKey]: d[sortKey],
                    'Persona asignada': d['Persona asignada'],
                    'Desarrollador': d['Desarrollador']
                }}));
            }}

            const top_late = getTop10(data, 'Liberación retrasada por');
            const top_dev_gt30 = getTop10(data, 'Estado Desarrollo > 30 días');
            const top_devlib_gt60 = getTop10(data, 'Desarrollo y liberada > 60 Días');

            return {{
                total_csrs,
                avg_delay: avg_delay,
                pct_late,
                avg_dev_gt30,
                num_dev_gt30,
                avg_devlib_gt60,
                num_devlib_gt60,
                max_delay,
                min_delay,
                by_estado,
                by_pr,
                by_tipo,
                by_persona,
                by_dev,
                delay_by_persona,
                delay_by_dev,
                created_trend,
                resolved_trend,
                top_late,
                top_dev_gt30,
                top_devlib_gt60,
            }};
        }}

        // Function to update the dashboard with new metrics
        function updateDashboard(metrics) {{
            // Update KPIs
            document.getElementById('kpiTotalCSRs').innerText = metrics.total_csrs;
            document.getElementById('kpiAvgDelay').innerText = metrics.avg_delay.toFixed(1);
            document.getElementById('kpiPctLate').innerText = metrics.pct_late.toFixed(1) + '%';
            document.getElementById('kpiNumDevGT30').innerText = metrics.num_dev_gt30;
            document.getElementById('kpiNumDevLibGT60').innerText = metrics.num_devlib_gt60;
            document.getElementById('kpiMaxDelay').innerText = metrics.max_delay.toFixed(1);
            document.getElementById('kpiMinDelay').innerText = metrics.min_delay.toFixed(1);

            // Update Pie Charts
            Plotly.react('estadoPie', [{{labels: Object.keys(metrics.by_estado), values: Object.values(metrics.by_estado), type: 'pie', hole: .5, textinfo: 'label+percent', marker: {{colors: ['#6a82fb', '#fc5c7d', '#4CAF50', '#FFC107', '#2196F3', '#FF5722']}}}}], {{title: 'Estado', height: 350}});
            Plotly.react('prPie', [{{labels: Object.keys(metrics.by_pr), values: Object.values(metrics.by_pr), type: 'pie', hole: .5, textinfo: 'label+percent', marker: {{colors: ['#6a82fb', '#fc5c7d', '#4CAF50', '#FFC107', '#2196F3', '#FF5722']}}}}], {{title: 'Prioridad', height: 350}});
            Plotly.react('tipoPie', [{{labels: Object.keys(metrics.by_tipo), values: Object.values(metrics.by_tipo), type: 'pie', hole: .5, textinfo: 'label+percent', marker: {{colors: ['#6a82fb', '#fc5c7d', '#4CAF50', '#FFC107', '#2196F3', '#FF5722']}}}}], {{title: 'Tipo', height: 350}});

            // Update Trend Chart
            Plotly.react('trendChart', [
                {{x: Object.keys(metrics.created_trend), y: Object.values(metrics.created_trend), name:'Created', type:'scatter', mode:'lines+markers', line:{{color:'#6a82fb'}}}},
                {{x: Object.keys(metrics.resolved_trend), y: Object.values(metrics.resolved_trend), name:'Released', type:'scatter', mode:'lines+markers', line:{{color:'#4CAF50'}}}}
            ], {{
                title: 'CSRs Created & Released by Week',
                xaxis: {{title: 'Week'}},
                yaxis: {{title: 'Number of CSRs'}},
                height: 400
            }});

            // Update Bar Charts (Workload)
            Plotly.react('personaBar', [{{x: Object.keys(metrics.by_persona), y: Object.values(metrics.by_persona), type:'bar', marker:{{color:'#6a82fb'}}}}], {{title: 'CSRs by Persona Asignada', height: 350}});
            Plotly.react('devBar', [{{x: Object.keys(metrics.by_dev), y: Object.values(metrics.by_dev), type:'bar', marker:{{color:'#fc5c7d'}}}}], {{title: 'CSRs by Desarrollador', height: 350}});

            // Update Bar Charts (Average Delay)
            Plotly.react('personaDelay', [{{x: Object.keys(metrics.delay_by_persona), y: Object.values(metrics.delay_by_persona).map(v => v.toFixed(1)), type:'bar', marker:{{color:'#6a82fb'}} }}], {{title: 'Avg Delay by Persona Asignada (Days)', height: 350}});
            Plotly.react('devDelay', [{{x: Object.keys(metrics.delay_by_dev), y: Object.values(metrics.delay_by_dev).map(v => v.toFixed(1)), type:'bar', marker:{{color:'#fc5c7d'}}}}], {{title: 'Avg Delay by Desarrollador (Days)', height: 350}});

            // Update Tables
            function updateTable(tableId, data, colNames) {{
                const tableBody = document.querySelector(`#${{tableId}} tbody`);
                tableBody.innerHTML = ''; // Clear existing rows
                data.forEach(row => {{
                    const tr = document.createElement('tr');
                    // Manually map to ensure correct order and format
                    const summary = row.Resumen || '';
                    const key = row.Clave || '';
                    const metricValue = typeof row[colNames[2]] === 'number' ? row[colNames[2]].toFixed(1) : (row[colNames[2]] || '0');
                    const assignee = row['Persona asignada'] || '';
                    const developer = row['Desarrollador'] || '';
                    
                    tr.innerHTML = `<td>${{summary}}</td><td>${{key}}</td><td>${{metricValue}}</td><td>${{assignee}}</td><td>${{developer}}</td>`;
                    tableBody.appendChild(tr);
                }});
            }}

            updateTable('tableLate', metrics.top_late, ['Resumen', 'Clave', 'Liberación retrasada por', 'Persona asignada', 'Desarrollador']);
            updateTable('tableDevGT30', metrics.top_dev_gt30, ['Resumen', 'Clave', 'Estado Desarrollo > 30 días', 'Persona asignada', 'Desarrollador']);
            updateTable('tableDevLibGT60', metrics.top_devlib_gt60, ['Resumen', 'Clave', 'Desarrollo y liberada > 60 Días', 'Persona asignada', 'Desarrollador']);
        }}

        function filterData() {{
            const filterEstado = document.getElementById('filterEstado').value;
            const filterPr = document.getElementById('filterPr').value;
            const filterTipo = document.getElementById('filterTipo').value;
            const filterPersona = document.getElementById('filterPersona').value;
            const filterDesarrollador = document.getElementById('filterDesarrollador').value;
            const startDateStr = document.getElementById('startDate').value;
            const endDateStr = document.getElementById('endDate').value;

            // Convert date strings to Date objects for comparison
            const startDate = startDateStr ? new Date(startDateStr) : null;
            // Set end date to end of the day for inclusive filtering
            const endDate = endDateStr ? new Date(endDateStr) : null;
            if (endDate) {{
                endDate.setHours(23, 59, 59, 999);
            }}

            currentData = originalData.filter(d => {{
                // Categorical filters
                const matchEstado = filterEstado === '' || d['Estado'] === filterEstado;
                const matchPr = filterPr === '' || d['Pr'] === filterPr;
                const matchTipo = filterTipo === '' || d['T'] === filterTipo;
                const matchPersona = filterPersona === '' || d['Persona asignada'] === filterPersona;
                const matchDesarrollador = filterDesarrollador === '' || d['Desarrollador'] === filterDesarrollador;

                // Date filter (Fecha Real de Liberación)
                let matchDate = true;
                if (d['Fecha Real de Liberación']) {{
                    const releaseDate = new Date(d['Fecha Real de Liberación']);
                    if (startDate && releaseDate < startDate) {{
                        matchDate = false;
                    }}
                    if (endDate && releaseDate > endDate) {{
                        matchDate = false;
                    }}
                }} else if (startDate || endDate) {{
                    // If a date filter is applied but the CSR has no release date, it doesn't match
                    matchDate = false;
                }}
                
                return matchEstado && matchPr && matchTipo && matchPersona && matchDesarrollador && matchDate;
            }});
            updateDashboard(calculateMetrics(currentData));
        }}

        function resetFilters() {{
            document.getElementById('filterEstado').value = '';
            document.getElementById('filterPr').value = '';
            document.getElementById('filterTipo').value = '';
            document.getElementById('filterPersona').value = '';
            document.getElementById('filterDesarrollador').value = '';
            
            // Reset date filters to min/max from original data
            const minReleaseDate = originalData.reduce((min, d) => {{
                if (d['Fecha Real de Liberación']) {{
                    const current = new Date(d['Fecha Real de Liberación']);
                    return min === null || current < min ? current : min;
                }}
                return min;
            }}, null);

            const maxReleaseDate = originalData.reduce((max, d) => {{
                if (d['Fecha Real de Liberación']) {{
                    const current = new Date(d['Fecha Real de Liberación']);
                    return max === null || current > max ? current : max;
                }}
                return max;
            }}, null);
            
            document.getElementById('startDate').value = minReleaseDate ? minReleaseDate.toISOString().split('T')[0] : '';
            document.getElementById('endDate').value = maxReleaseDate ? maxReleaseDate.toISOString().split('T')[0] : '';

            currentData = originalData;
            updateDashboard(calculateMetrics(currentData));
        }}

        // Initial dashboard render
        document.addEventListener('DOMContentLoaded', () => {{
            // Set initial date filter values based on data range
            const initialMetrics = calculateMetrics(originalData);

            // Dynamically set date inputs based on data
            const releaseDates = originalData
                                    .map(d => d['Fecha Real de Liberación'])
                                    .filter(d => d); // Filter out empty/null dates
            
            let minDate = '';
            let maxDate = '';

            if (releaseDates.length > 0) {{
                const dates = releaseDates.map(dateStr => new Date(dateStr));
                const min = new Date(Math.min(...dates));
                const max = new Date(Math.max(...dates));
                minDate = min.toISOString().split('T')[0];
                maxDate = max.toISOString().split('T')[0];
            }}

            document.getElementById('startDate').value = minDate;
            document.getElementById('endDate').value = maxDate;

            updateDashboard(initialMetrics);
        }});

    </script>
</body>
</html>
"""
    with open(HTML_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Dashboard generated successfully at {HTML_FILE}")

# =============== MAIN EXECUTION ===============

if __name__ == "__main__":
    df = load_data()
    df = preprocess_data(df)
    metrics = get_metrics(df)
    render_dashboard(df, metrics)