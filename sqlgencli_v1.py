“””
AWS Redshift SQL Code Generator with CLI
Uses Polars, Jinja2, and Typer for enhanced functionality
“””

import polars as pl
import typer
from typing import Optional, List
from pathlib import Path
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, Template
import sqlglot
from sqlglot import parse_one
from enum import Enum
import yaml
from rich.console import Console
from rich.table import Table
from rich import print as rprint

app = typer.Typer(help=“AWS Redshift SQL Generator and Documentation Tool”)
console = Console()

class DocumentFormat(str, Enum):
yaml = “yaml”
markdown = “markdown”
html = “html”

class SCDType(str, Enum):
TYPE1 = “TYPE1”
TYPE2 = “TYPE2”
INSERT = “INSERT”

# Jinja2 Templates as strings (in production, use external files)

DDL_TEMPLATE = “””– AWS Redshift DDL Scripts
– Generated on: {{ generation_date }}
– {{ ‘=’*70 }}

{% for table in tables %}
– Table: {{ table.schema_name }}.{{ table.table_name }}
{% if table.description %}– Description: {{ table.description }}{% endif %}
DROP TABLE IF EXISTS {{ table.schema_name }}.{{ table.table_name }} CASCADE;

CREATE TABLE {{ table.schema_name }}.{{ table.table_name }} (
{%- for col in table.columns %}
{{ col.column_name }} {{ col.data_type }}
{%- if col.not_null %} NOT NULL{% endif %}
{%- if col.default_value %} DEFAULT {{ col.default_value }}{% endif %}
{%- if col.encode %} ENCODE {{ col.encode }}{% endif %}
{%- if not loop.last %},{% endif %}
{%- endfor %}
{%- if table.primary_key %},
PRIMARY KEY ({{ table.primary_key }})
{%- endif %}
)
{%- if table.dist_style %}
{%- if table.dist_style == ‘KEY’ and table.dist_key %}
DISTKEY({{ table.dist_key }})
{%- else %}
DISTSTYLE {{ table.dist_style }}
{%- endif %}
{%- endif %}
{%- if table.sort_keys %}
{{ table.sort_type|default(‘COMPOUND’) }} SORTKEY({{ table.sort_keys }})
{%- endif %};

{%- if table.description %}
COMMENT ON TABLE {{ table.schema_name }}.{{ table.table_name }} IS ‘{{ table.description|replace(”’”, “’’”) }}’;
{%- endif %}

{% endfor %}
“””

TYPE1_TEMPLATE = “””– SCD Type 1: Update existing, Insert new
– Target: {{ target_table }}
BEGIN TRANSACTION;

MERGE INTO {{ target_table }} AS tgt
USING (
SELECT
{%- for col in columns %}
{{ col.source_expr }} AS {{ col.target_column }}
{%- if not loop.last %},{% endif %}
{%- endfor %}
FROM {{ source_table }}
WHERE 1=1  – Add incremental filter here (e.g., WHERE load_date > last_load_date)
) AS src
ON {{ business_key_join }}
WHEN MATCHED THEN
UPDATE SET
{%- for col in update_columns %}
tgt.{{ col }} = src.{{ col }}
{%- if not loop.last %},{% endif %}
{%- endfor %},
tgt.updated_date = GETDATE()
WHEN NOT MATCHED THEN
INSERT ({{ target_column_list }}, created_date, updated_date)
VALUES ({{ source_column_list }}, GETDATE(), GETDATE());

COMMIT;
“””

TYPE2_TEMPLATE = “””– SCD Type 2: Track history with effective dates
– Target: {{ target_table }}
BEGIN TRANSACTION;

– Expire changed records
UPDATE {{ target_table }} AS tgt
SET
tgt.effective_end_date = DATEADD(day, -1, GETDATE()),
tgt.is_current = FALSE,
tgt.updated_date = GETDATE()
FROM (
SELECT {{ business_key_list }}
FROM {{ source_table }}
) AS src
WHERE tgt.is_current = TRUE
AND {{ business_key_join }}
AND ({{ change_detection }});

– Insert new versions (changed records and new records)
INSERT INTO {{ target_table }} (
{{ target_column_list }},
effective_start_date,
effective_end_date,
is_current,
created_date
)
SELECT
{%- for col in columns %}
{{ col.source_expr }} AS {{ col.target_column }},
{%- endfor %}
GETDATE() AS effective_start_date,
‘9999-12-31’::DATE AS effective_end_date,
TRUE AS is_current,
GETDATE() AS created_date
FROM {{ source_table }} AS src
WHERE NOT EXISTS (
SELECT 1 FROM {{ target_table }} AS tgt
WHERE tgt.is_current = TRUE
AND {{ business_key_join }}
)
OR EXISTS (
SELECT 1 FROM {{ target_table }} AS tgt
WHERE tgt.is_current = TRUE
AND {{ business_key_join }}
AND ({{ change_detection }})
);

COMMIT;
“””

INSERT_TEMPLATE = “””– Insert load
– Target: {{ target_table }}
INSERT INTO {{ target_table }} (
{{ target_column_list }}
)
SELECT
{%- for col in columns %}
{{ col.source_expr }} AS {{ col.target_column }}
{%- if not loop.last %},{% endif %}
{%- endfor %}
FROM {{ source_table }}
WHERE 1=1;  – Add filter conditions here
“””

MARKDOWN_DOC_TEMPLATE = “””# Database Documentation
**Generated:** {{ generation_date }}

## Table of Contents

{% for table in tables %}

- [{{ table.schema_name }}.{{ table.table_name }}](#{{ table.schema_name }}-{{ table.table_name }})
  {%- endfor %}

-----

{% for table in tables %}

## {{ table.schema_name }}.{{ table.table_name }}

**Description:** {{ table.description|default(‘N/A’) }}

**Properties:**

- **Primary Key:** {{ table.primary_key|default(‘N/A’) }}
- **Distribution Style:** {{ table.dist_style|default(‘N/A’) }}
- **Distribution Key:** {{ table.dist_key|default(‘N/A’) }}
- **Sort Keys:** {{ table.sort_keys|default(‘N/A’) }}
- **Sort Type:** {{ table.sort_type|default(‘N/A’) }}

### Columns

|Column Name                    |Data Type          |Nullable                                 |Default             |Encoding         |Description  |
|-------------------------------|-------------------|-----------------------------------------|--------------------|-----------------|-------------|
|{%- for col in table.columns %}|                   |                                         |                    |                 |             |
|{{ col.column_name }}          |{{ col.data_type }}|{{ ‘Yes’ if not col.not_null else ‘No’ }}|{{ col.default_value|default(‘N/A’) }}|{{ col.encode|
|{%- endfor %}                  |                   |                                         |                    |                 |             |

### Data Lineage

```mermaid
{{ table.lineage_diagram }}
```

-----

{% endfor %}
“””

HTML_DOC_TEMPLATE = “””<!DOCTYPE html>

<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Database Documentation</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 40px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        h1 { color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }
        h2 { color: #34495e; margin-top: 30px; border-bottom: 2px solid #ecf0f1; padding-bottom: 8px; }
        h3 { color: #7f8c8d; }
        table { width: 100%; border-collapse: collapse; margin: 20px 0; }
        th { background: #3498db; color: white; padding: 12px; text-align: left; }
        td { padding: 10px; border-bottom: 1px solid #ecf0f1; }
        tr:hover { background: #f8f9fa; }
        .property { background: #ecf0f1; padding: 8px; margin: 5px 0; border-radius: 4px; }
        .property strong { color: #2c3e50; }
        .mermaid { background: white; padding: 20px; border-radius: 4px; margin: 20px 0; }
        .toc { background: #ecf0f1; padding: 20px; border-radius: 4px; margin: 20px 0; }
        .toc ul { list-style: none; padding-left: 0; }
        .toc li { padding: 5px 0; }
        .toc a { text-decoration: none; color: #3498db; }
        .toc a:hover { text-decoration: underline; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🗄️ Database Documentation</h1>
        <p><strong>Generated:</strong> {{ generation_date }}</p>

```
    <div class="toc">
        <h2>📑 Table of Contents</h2>
        <ul>
        {%- for table in tables %}
            <li><a href="#{{ table.schema_name }}-{{ table.table_name }}">{{ table.schema_name }}.{{ table.table_name }}</a></li>
        {%- endfor %}
        </ul>
    </div>

    {% for table in tables %}
    <div id="{{ table.schema_name }}-{{ table.table_name }}">
        <h2>{{ table.schema_name }}.{{ table.table_name }}</h2>
        <p>{{ table.description|default('No description available.') }}</p>
        
        <h3>Properties</h3>
        <div class="property"><strong>Primary Key:</strong> {{ table.primary_key|default('N/A') }}</div>
        <div class="property"><strong>Distribution Style:</strong> {{ table.dist_style|default('N/A') }}</div>
        <div class="property"><strong>Distribution Key:</strong> {{ table.dist_key|default('N/A') }}</div>
        <div class="property"><strong>Sort Keys:</strong> {{ table.sort_keys|default('N/A') }}</div>
        <div class="property"><strong>Sort Type:</strong> {{ table.sort_type|default('N/A') }}</div>
        
        <h3>Columns</h3>
        <table>
            <thead>
                <tr>
                    <th>Column Name</th>
                    <th>Data Type</th>
                    <th>Nullable</th>
                    <th>Default</th>
                    <th>Encoding</th>
                </tr>
            </thead>
            <tbody>
            {%- for col in table.columns %}
                <tr>
                    <td><strong>{{ col.column_name }}</strong></td>
                    <td>{{ col.data_type }}</td>
                    <td>{{ 'Yes' if not col.not_null else 'No' }}</td>
                    <td>{{ col.default_value|default('N/A') }}</td>
                    <td>{{ col.encode|default('N/A') }}</td>
                </tr>
            {%- endfor %}
            </tbody>
        </table>
        
        <h3>Data Lineage</h3>
        <div class="mermaid">
```

{{ table.lineage_diagram }}
</div>
</div>
<hr>
{% endfor %}
</div>

```
<script>
    mermaid.initialize({ startOnLoad: true, theme: 'default' });
</script>
```

</body>
</html>
"""

class RedshiftSQLGenerator:
“”“Generate and validate Redshift SQL scripts using Polars and Jinja2”””

```
def __init__(self):
    self.tables_df: Optional[pl.DataFrame] = None
    self.columns_df: Optional[pl.DataFrame] = None
    self.mappings_df: Optional[pl.DataFrame] = None
    self.errors: List[str] = []
    
    # Initialize Jinja2 environment
    self.jinja_env = Environment(autoescape=False)
    
def load_definitions(self, table_def_file: Path, mapping_file: Path) -> bool:
    """Load Excel files into Polars DataFrames"""
    try:
        # Load table definitions
        self.tables_df = pl.read_excel(table_def_file, sheet_name='Tables')
        self.columns_df = pl.read_excel(table_def_file, sheet_name='Columns')
        
        # Load source-to-target mappings
        self.mappings_df = pl.read_excel(mapping_file, sheet_name='Mappings')
        
        console.print("✓ Excel files loaded successfully", style="green")
        return True
    except Exception as e:
        self.errors.append(f"Error loading Excel files: {str(e)}")
        console.print(f"✗ Error loading files: {str(e)}", style="red")
        return False

def generate_ddl(self, output_file: Path) -> bool:
    """Generate DDL scripts using Jinja2 template"""
    if self.tables_df is None or self.columns_df is None:
        self.errors.append("Data not loaded")
        return False
    
    try:
        # Prepare data for template
        tables_data = []
        
        for table_row in self.tables_df.iter_rows(named=True):
            # Get columns for this table
            table_cols = self.columns_df.filter(
                (pl.col('schema_name') == table_row['schema_name']) &
                (pl.col('table_name') == table_row['table_name'])
            ).sort('column_order')
            
            columns = [dict(row) for row in table_cols.iter_rows(named=True)]
            
            table_data = dict(table_row)
            table_data['columns'] = columns
            tables_data.append(table_data)
        
        # Render template
        template = self.jinja_env.from_string(DDL_TEMPLATE)
        ddl_content = template.render(
            tables=tables_data,
            generation_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
        
        # Write to file
        output_file.write_text(ddl_content)
        console.print(f"✓ DDL scripts generated: {output_file}", style="green")
        return True
        
    except Exception as e:
        self.errors.append(f"Error generating DDL: {str(e)}")
        console.print(f"✗ Error generating DDL: {str(e)}", style="red")
        return False

def generate_dml(self, output_file: Path) -> bool:
    """Generate DML scripts using Jinja2 templates"""
    if self.mappings_df is None:
        self.errors.append("Mappings not loaded")
        return False
    
    try:
        dml_scripts = [
            "-- AWS Redshift DML Scripts (Incremental Load)",
            f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "-- " + "="*70 + "\n"
        ]
        
        # Group by target table
        unique_targets = self.mappings_df.select([
            'target_schema', 'target_table'
        ]).unique()
        
        for target_row in unique_targets.iter_rows(named=True):
            target_schema = target_row['target_schema']
            target_table = target_row['target_table']
            full_target = f"{target_schema}.{target_table}"
            
            # Get mappings for this table
            table_mappings = self.mappings_df.filter(
                (pl.col('target_schema') == target_schema) &
                (pl.col('target_table') == target_table)
            ).sort('target_column_order')
            
            # Get SCD type
            scd_type = table_mappings.select('scd_type').to_series()[0]
            
            # Generate appropriate script
            if scd_type == 'TYPE1':
                script = self._generate_type1_load(table_mappings, full_target)
            elif scd_type == 'TYPE2':
                script = self._generate_type2_load(table_mappings, full_target)
            else:
                script = self._generate_insert_load(table_mappings, full_target)
            
            dml_scripts.append(f"\n-- Load: {full_target} (SCD {scd_type})")
            dml_scripts.append("-- " + "-"*70)
            dml_scripts.append(script)
        
        # Write to file
        dml_content = "\n".join(dml_scripts)
        output_file.write_text(dml_content)
        console.print(f"✓ DML scripts generated: {output_file}", style="green")
        return True
        
    except Exception as e:
        self.errors.append(f"Error generating DML: {str(e)}")
        console.print(f"✗ Error generating DML: {str(e)}", style="red")
        return False

def _generate_type1_load(self, mappings: pl.DataFrame, target_table: str) -> str:
    """Generate SCD Type 1 using Jinja2 template"""
    first_row = mappings.row(0, named=True)
    source_table = f"{first_row['source_schema']}.{first_row['source_table']}"
    
    columns = []
    business_keys = []
    update_cols = []
    
    for row in mappings.iter_rows(named=True):
        target_col = row['target_column']
        
        # Determine source expression
        if row.get('transformation'):
            source_expr = row['transformation']
        elif row.get('source_column'):
            source_expr = f"src.{row['source_column']}"
        elif row.get('constant_value'):
            const_val = row['constant_value']
            source_expr = f"'{const_val}'" if isinstance(const_val, str) else str(const_val)
        else:
            source_expr = "NULL"
        
        columns.append({
            'target_column': target_col,
            'source_expr': source_expr
        })
        
        if row.get('is_business_key'):
            business_keys.append(target_col)
        else:
            update_cols.append(target_col)
    
    business_key_join = ' AND '.join([f'tgt.{k} = src.{k}' for k in business_keys])
    
    template = self.jinja_env.from_string(TYPE1_TEMPLATE)
    return template.render(
        target_table=target_table,
        source_table=source_table,
        columns=columns,
        business_key_join=business_key_join,
        update_columns=update_cols,
        target_column_list=', '.join([c['target_column'] for c in columns]),
        source_column_list=', '.join([f"src.{c['target_column']}" for c in columns])
    )

def _generate_type2_load(self, mappings: pl.DataFrame, target_table: str) -> str:
    """Generate SCD Type 2 using Jinja2 template"""
    first_row = mappings.row(0, named=True)
    source_table = f"{first_row['source_schema']}.{first_row['source_table']}"
    
    columns = []
    business_keys = []
    compare_cols = []
    
    for row in mappings.iter_rows(named=True):
        target_col = row['target_column']
        
        if row.get('transformation'):
            source_expr = row['transformation']
        elif row.get('source_column'):
            source_expr = f"src.{row['source_column']}"
        elif row.get('constant_value'):
            const_val = row['constant_value']
            source_expr = f"'{const_val}'" if isinstance(const_val, str) else str(const_val)
        else:
            source_expr = "NULL"
        
        columns.append({
            'target_column': target_col,
            'source_expr': source_expr
        })
        
        if row.get('is_business_key'):
            business_keys.append(target_col)
        else:
            compare_cols.append(target_col)
    
    business_key_join = ' AND '.join([f'tgt.{k} = src.{k}' for k in business_keys])
    business_key_list = ', '.join([f'src.{k}' for k in business_keys])
    change_detection = ' OR '.join([
        f"NVL(tgt.{c}, 'NULL') <> NVL(src.{c}, 'NULL')" 
        for c in compare_cols[:5]
    ])
    
    template = self.jinja_env.from_string(TYPE2_TEMPLATE)
    return template.render(
        target_table=target_table,
        source_table=source_table,
        columns=columns,
        business_key_join=business_key_join,
        business_key_list=business_key_list,
        change_detection=change_detection,
        target_column_list=', '.join([c['target_column'] for c in columns])
    )

def _generate_insert_load(self, mappings: pl.DataFrame, target_table: str) -> str:
    """Generate INSERT using Jinja2 template"""
    first_row = mappings.row(0, named=True)
    source_table = f"{first_row['source_schema']}.{first_row['source_table']}"
    
    columns = []
    
    for row in mappings.iter_rows(named=True):
        target_col = row['target_column']
        
        if row.get('transformation'):
            source_expr = row['transformation']
        elif row.get('source_column'):
            source_expr = row['source_column']
        elif row.get('constant_value'):
            const_val = row['constant_value']
            source_expr = f"'{const_val}'" if isinstance(const_val, str) else str(const_val)
        else:
            source_expr = "NULL"
        
        columns.append({
            'target_column': target_col,
            'source_expr': source_expr
        })
    
    template = self.jinja_env.from_string(INSERT_TEMPLATE)
    return template.render(
        target_table=target_table,
        source_table=source_table,
        columns=columns,
        target_column_list=', '.join([c['target_column'] for c in columns])
    )

def _build_lineage_diagram(self, table_schema: str, table_name: str) -> str:
    """Build Mermaid.js lineage diagram for a table"""
    if self.mappings_df is None:
        return "graph LR\n    No_Lineage[No lineage data available]"
    
    # Get mappings for this table
    table_mappings = self.mappings_df.filter(
        (pl.col('target_schema') == table_schema) &
        (pl.col('target_table') == table_name)
    )
    
    if table_mappings.height == 0:
        return "graph LR\n    No_Lineage[No lineage data available]"
    
    # Build diagram
    lines = ["graph LR"]
    
    # Get unique source tables
    sources = table_mappings.select(['source_schema', 'source_table']).unique()
    
    for idx, source_row in enumerate(sources.iter_rows(named=True)):
        source_id = f"SRC{idx}"
        source_name = f"{source_row['source_schema']}.{source_row['source_table']}"
        target_id = "TGT"
        target_name = f"{table_schema}.{table_name}"
        
        lines.append(f"    {source_id}[{source_name}]")
        lines.append(f"    {target_id}[{target_name}]")
        lines.append(f"    {source_id} -->|ETL Process| {target_id}")
    
    return "\n".join(lines)

def generate_documentation(self, output_file: Path, format: DocumentFormat) -> bool:
    """Generate documentation in YAML, Markdown, or HTML format"""
    if self.tables_df is None or self.columns_df is None:
        self.errors.append("Data not loaded")
        return False
    
    try:
        # Prepare data
        tables_data = []
        
        for table_row in self.tables_df.iter_rows(named=True):
            schema = table_row['schema_name']
            table = table_row['table_name']
            
            # Get columns
            table_cols = self.columns_df.filter(
                (pl.col('schema_name') == schema) &
                (pl.col('table_name') == table)
            ).sort('column_order')
            
            columns = [dict(row) for row in table_cols.iter_rows(named=True)]
            
            # Build lineage
            lineage = self._build_lineage_diagram(schema, table)
            
            table_data = dict(table_row)
            table_data['columns'] = columns
            table_data['lineage_diagram'] = lineage
            tables_data.append(table_data)
        
        # Generate documentation based on format
        if format == DocumentFormat.yaml:
            content = yaml.dump({
                'database_documentation': {
                    'generated': datetime.now().isoformat(),
                    'tables': tables_data
                }
            }, default_flow_style=False, sort_keys=False)
        
        elif format == DocumentFormat.markdown:
            template = self.jinja_env.from_string(MARKDOWN_DOC_TEMPLATE)
            content = template.render(
                tables=tables_data,
                generation_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
        
        elif format == DocumentFormat.html:
            template = self.jinja_env.from_string(HTML_DOC_TEMPLATE)
            content = template.render(
                tables=tables_data,
                generation_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )
        
        # Write to file
        output_file.write_text(content)
        console.print(f"✓ Documentation generated: {output_file}", style="green")
        return True
        
    except Exception as e:
        self.errors.append(f"Error generating documentation: {str(e)}")
        console.print(f"✗ Error generating documentation: {str(e)}", style="red")
        return False
```

def validate_sql_file(sql_file: Path) -> bool:
“”“Validate SQL syntax using sqlglot”””
console.print(f”\n[bold]Validating SQL file:[/bold] {sql_file}”)

```
try:
    sql_content = sql_file.read_text()
    
    # Split into statements
    statements = [s.strip() for s in sql_content.split(';') 
                 if s.strip() and not s.strip().startswith('--')]
    
    valid_count = 0
    error_count = 0
    errors = []
    
    for i, stmt in enumerate(statements, 1):
        if not stmt or len(stmt) < 10:
            continue
        
        try:
            parsed = parse_one(stmt, dialect='redshift')
            valid_count += 1
        except Exception as e:
            error_count += 1
            error_msg = f"Statement {i}: {str(e)[:100]}"
            errors.append(error_msg)
            console.print(f"  ✗ {error_msg}", style="red")
    
    # Summary table
    table = Table(title="Validation Results")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="magenta")
    
    table.add_row("Valid Statements", str(valid_count))
    table.add_row("Invalid Statements", str(error_count))
    table.add_row("Total Statements", str(valid_count + error_count))
    
    console.print(table)
    
    return error_count == 0
    
except Exception as e:
    console.print(f"✗ Validation failed: {str(e)}", style="red")
    return False
```

def create_sample_excel_files():
“”“Create sample Excel files”””
tables_data = {
‘schema_name’: [‘dwh’, ‘dwh’, ‘dwh’],
‘table_name’: [‘dim_customer’, ‘dim_product’, ‘fact_sales’],
‘description’: [
‘Customer dimension table’,
‘Product dimension table’,
‘Sales fact table’
],
‘primary_key’: [‘customer_key’, ‘product_key’, ‘sales_key’],
‘dist_style’: [‘KEY’, ‘ALL’, ‘KEY’],
‘dist_key’: [‘customer_key’, None, ‘customer_key’],
‘sort_keys’: [‘customer_id’, ‘product_id’, ‘sale_date, customer_key’],
‘sort_type’: [‘COMPOUND’, ‘COMPOUND’, ‘COMPOUND’]
}

```
columns_data = {
    'schema_name': ['dwh']*10,
    'table_name': ['dim_customer']*5 + ['dim_product']*5,
    'column_name': ['customer_key', 'customer_id', 'customer_name', 'email', 'created_date',
                    'product_key', 'product_id', 'product_name', 'price', 'created_date'],
    'column_order': [1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
    'data_type': ['BIGINT', 'VARCHAR(50)', 'VARCHAR(200)', 'VARCHAR(200)', 'TIMESTAMP',
                  'BIGINT', 'VARCHAR(50)', 'VARCHAR(200)', 'DECIMAL(10,2)', 'TIMESTAMP'],
    'not_null': [True, True, True, False, True, True, True, True, True, True],
```