“””
AWS Redshift SQL Code Generator
Generates DDL and DML scripts from Excel definitions with validation
“””

import pandas as pd
import sqlglot
from sqlglot import parse_one, exp
from sqlglot.dialects import redshift
from typing import List, Dict, Tuple
from pathlib import Path
from datetime import datetime

class RedshiftSQLGenerator:
“”“Generate and validate Redshift SQL scripts from Excel definitions”””

```
def __init__(self, table_def_file: str, mapping_file: str):
    """
    Initialize generator with Excel file paths
    
    Args:
        table_def_file: Path to table definitions Excel
        mapping_file: Path to source-to-target mapping Excel
    """
    self.table_def_file = table_def_file
    self.mapping_file = mapping_file
    self.tables_df = None
    self.columns_df = None
    self.mappings_df = None
    self.errors = []
    
def load_definitions(self):
    """Load Excel files into DataFrames"""
    try:
        # Load table definitions (multiple sheets)
        self.tables_df = pd.read_excel(self.table_def_file, sheet_name='Tables')
        self.columns_df = pd.read_excel(self.table_def_file, sheet_name='Columns')
        
        # Load source-to-target mappings
        self.mappings_df = pd.read_excel(self.mapping_file, sheet_name='Mappings')
        
        print("✓ Excel files loaded successfully")
        return True
    except Exception as e:
        self.errors.append(f"Error loading Excel files: {str(e)}")
        return False

def generate_ddl(self, output_file: str = 'ddl_scripts.sql'):
    """Generate DDL scripts for all tables"""
    if self.tables_df is None or self.columns_df is None:
        self.errors.append("Data not loaded. Call load_definitions() first")
        return False
    
    ddl_scripts = []
    ddl_scripts.append("-- AWS Redshift DDL Scripts")
    ddl_scripts.append(f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    ddl_scripts.append("-- " + "="*70 + "\n")
    
    # Group columns by table
    for _, table in self.tables_df.iterrows():
        schema = table['schema_name']
        table_name = table['table_name']
        full_table = f"{schema}.{table_name}"
        
        ddl_scripts.append(f"\n-- Table: {full_table}")
        ddl_scripts.append(f"DROP TABLE IF EXISTS {full_table} CASCADE;")
        
        # Get columns for this table
        table_columns = self.columns_df[
            (self.columns_df['schema_name'] == schema) & 
            (self.columns_df['table_name'] == table_name)
        ].sort_values('column_order')
        
        if table_columns.empty:
            self.errors.append(f"No columns defined for {full_table}")
            continue
        
        # Build CREATE TABLE statement
        create_stmt = [f"CREATE TABLE {full_table} ("]
        
        # Add columns
        col_defs = []
        for _, col in table_columns.iterrows():
            col_def = f"    {col['column_name']} {col['data_type']}"
            
            # Add constraints
            if pd.notna(col.get('not_null')) and col['not_null']:
                col_def += " NOT NULL"
            if pd.notna(col.get('default_value')):
                col_def += f" DEFAULT {col['default_value']}"
            if pd.notna(col.get('encode')):
                col_def += f" ENCODE {col['encode']}"
            
            col_defs.append(col_def)
        
        create_stmt.append(",\n".join(col_defs))
        
        # Add primary key if defined
        if pd.notna(table.get('primary_key')):
            pk_cols = table['primary_key']
            create_stmt.append(f",\n    PRIMARY KEY ({pk_cols})")
        
        create_stmt.append(")")
        
        # Add table properties
        table_props = []
        
        # Distribution style and key
        if pd.notna(table.get('dist_style')):
            if table['dist_style'].upper() == 'KEY' and pd.notna(table.get('dist_key')):
                table_props.append(f"DISTKEY({table['dist_key']})")
            else:
                table_props.append(f"DISTSTYLE {table['dist_style'].upper()}")
        
        # Sort keys
        if pd.notna(table.get('sort_keys')):
            sort_type = table.get('sort_type', 'COMPOUND').upper()
            table_props.append(f"{sort_type} SORTKEY({table['sort_keys']})")
        
        if table_props:
            create_stmt.append("\n" + "\n".join(table_props))
        
        create_stmt.append(";")
        
        ddl_scripts.append("\n".join(create_stmt))
        
        # Add table comment if provided
        if pd.notna(table.get('description')):
            comment = table['description'].replace("'", "''")
            ddl_scripts.append(
                f"COMMENT ON TABLE {full_table} IS '{comment}';"
            )
    
    # Write to file
    ddl_content = "\n".join(ddl_scripts)
    with open(output_file, 'w') as f:
        f.write(ddl_content)
    
    print(f"✓ DDL scripts generated: {output_file}")
    return True

def generate_dml(self, output_file: str = 'dml_scripts.sql'):
    """Generate DML scripts for incremental loads"""
    if self.mappings_df is None:
        self.errors.append("Mappings not loaded. Call load_definitions() first")
        return False
    
    dml_scripts = []
    dml_scripts.append("-- AWS Redshift DML Scripts (Incremental Load)")
    dml_scripts.append(f"-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    dml_scripts.append("-- " + "="*70 + "\n")
    
    # Group mappings by target table
    for target_table in self.mappings_df['target_table'].unique():
        target_schema = self.mappings_df[
            self.mappings_df['target_table'] == target_table
        ]['target_schema'].iloc[0]
        
        full_target = f"{target_schema}.{target_table}"
        
        table_mappings = self.mappings_df[
            self.mappings_df['target_table'] == target_table
        ].sort_values('target_column_order')
        
        # Get SCD type
        scd_type = table_mappings['scd_type'].iloc[0] if 'scd_type' in table_mappings.columns else 'TYPE1'
        
        dml_scripts.append(f"\n-- Load: {full_target} (SCD {scd_type})")
        dml_scripts.append("-- " + "-"*70)
        
        if scd_type == 'TYPE1':
            script = self._generate_type1_load(table_mappings, full_target)
        elif scd_type == 'TYPE2':
            script = self._generate_type2_load(table_mappings, full_target)
        else:
            script = self._generate_insert_load(table_mappings, full_target)
        
        dml_scripts.append(script)
    
    # Write to file
    dml_content = "\n".join(dml_scripts)
    with open(output_file, 'w') as f:
        f.write(dml_content)
    
    print(f"✓ DML scripts generated: {output_file}")
    return True

def _generate_type1_load(self, mappings: pd.DataFrame, target_table: str) -> str:
    """Generate SCD Type 1 MERGE statement"""
    source_table = f"{mappings['source_schema'].iloc[0]}.{mappings['source_table'].iloc[0]}"
    
    # Build column mappings
    target_cols = []
    source_exprs = []
    update_sets = []
    business_keys = []
    
    for _, row in mappings.iterrows():
        target_col = row['target_column']
        target_cols.append(target_col)
        
        # Determine source expression
        if pd.notna(row.get('transformation')):
            source_expr = row['transformation']
        elif pd.notna(row.get('source_column')):
            source_expr = f"src.{row['source_column']}"
        elif pd.notna(row.get('constant_value')):
            const_val = row['constant_value']
            source_expr = f"'{const_val}'" if isinstance(const_val, str) else str(const_val)
        else:
            source_expr = "NULL"
        
        source_exprs.append(f"{source_expr} AS {target_col}")
        
        # Add to update set if not a business key
        if pd.notna(row.get('is_business_key')) and row['is_business_key']:
            business_keys.append(target_col)
        else:
            update_sets.append(f"    tgt.{target_col} = src.{target_col}")
    
    # Build MERGE statement
    merge_sql = f"""
```

– SCD Type 1: Update existing, Insert new
BEGIN TRANSACTION;

MERGE INTO {target_table} AS tgt
USING (
SELECT
{’,\n        ‘.join(source_exprs)}
FROM {source_table}
WHERE 1=1  – Add incremental filter here
) AS src
ON {’ AND ‘.join([f’tgt.{k} = src.{k}’ for k in business_keys])}
WHEN MATCHED THEN
UPDATE SET
{’,\n’.join(update_sets)},
tgt.updated_date = GETDATE()
WHEN NOT MATCHED THEN
INSERT ({’, ‘.join(target_cols)}, created_date, updated_date)
VALUES ({’, ‘.join([f’src.{c}’ for c in target_cols])}, GETDATE(), GETDATE());

COMMIT;
“””
return merge_sql

```
def _generate_type2_load(self, mappings: pd.DataFrame, target_table: str) -> str:
    """Generate SCD Type 2 load with history tracking"""
    source_table = f"{mappings['source_schema'].iloc[0]}.{mappings['source_table'].iloc[0]}"
    
    target_cols = []
    source_exprs = []
    business_keys = []
    compare_cols = []
    
    for _, row in mappings.iterrows():
        target_col = row['target_column']
        target_cols.append(target_col)
        
        if pd.notna(row.get('transformation')):
            source_expr = row['transformation']
        elif pd.notna(row.get('source_column')):
            source_expr = f"src.{row['source_column']}"
        elif pd.notna(row.get('constant_value')):
            const_val = row['constant_value']
            source_expr = f"'{const_val}'" if isinstance(const_val, str) else str(const_val)
        else:
            source_expr = "NULL"
        
        source_exprs.append(f"{source_expr} AS {target_col}")
        
        if pd.notna(row.get('is_business_key')) and row['is_business_key']:
            business_keys.append(target_col)
        else:
            compare_cols.append(target_col)
    
    # Build comparison for changes
    change_check = ' OR '.join([
        f"NVL(tgt.{c}, 'NULL') <> NVL(src.{c}, 'NULL')" 
        for c in compare_cols[:5]  # Limit for readability
    ])
    
    type2_sql = f"""
```

– SCD Type 2: Track history with effective dates
BEGIN TRANSACTION;

– Expire changed records
UPDATE {target_table} AS tgt
SET
tgt.effective_end_date = DATEADD(day, -1, GETDATE()),
tgt.is_current = FALSE,
tgt.updated_date = GETDATE()
FROM (
SELECT {’, ‘.join([f’src.{k}’ for k in business_keys])}
FROM {source_table} AS src
) AS src
WHERE tgt.is_current = TRUE
AND {’ AND ‘.join([f’tgt.{k} = src.{k}’ for k in business_keys])}
AND ({change_check});

– Insert new versions
INSERT INTO {target_table} (
{’, ‘.join(target_cols)},
effective_start_date,
effective_end_date,
is_current,
created_date
)
SELECT
{’,\n    ‘.join(source_exprs)},
GETDATE() AS effective_start_date,
‘9999-12-31’::DATE AS effective_end_date,
TRUE AS is_current,
GETDATE() AS created_date
FROM {source_table} AS src
WHERE NOT EXISTS (
SELECT 1 FROM {target_table} AS tgt
WHERE tgt.is_current = TRUE
AND {’ AND ‘.join([f’tgt.{k} = src.{k}’ for k in business_keys])}
)
OR EXISTS (
SELECT 1 FROM {target_table} AS tgt
WHERE tgt.is_current = TRUE
AND {’ AND ‘.join([f’tgt.{k} = src.{k}’ for k in business_keys])}
AND ({change_check})
);

COMMIT;
“””
return type2_sql

```
def _generate_insert_load(self, mappings: pd.DataFrame, target_table: str) -> str:
    """Generate simple INSERT statement"""
    source_table = f"{mappings['source_schema'].iloc[0]}.{mappings['source_table'].iloc[0]}"
    
    target_cols = []
    source_exprs = []
    
    for _, row in mappings.iterrows():
        target_col = row['target_column']
        target_cols.append(target_col)
        
        if pd.notna(row.get('transformation')):
            source_expr = row['transformation']
        elif pd.notna(row.get('source_column')):
            source_expr = row['source_column']
        elif pd.notna(row.get('constant_value')):
            const_val = row['constant_value']
            source_expr = f"'{const_val}'" if isinstance(const_val, str) else str(const_val)
        else:
            source_expr = "NULL"
        
        source_exprs.append(source_expr)
    
    insert_sql = f"""
```

– Insert load
INSERT INTO {target_table} (
{’, ‘.join(target_cols)}
)
SELECT
{’,\n    ’.join(source_exprs)}
FROM {source_table}
WHERE 1=1;  – Add filter conditions here
“””
return insert_sql

```
def validate_sql(self, sql_file: str) -> bool:
    """Validate SQL syntax using sqlglot"""
    print(f"\nValidating SQL file: {sql_file}")
    
    try:
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        # Split into individual statements
        statements = [s.strip() for s in sql_content.split(';') if s.strip() and not s.strip().startswith('--')]
        
        valid_count = 0
        error_count = 0
        
        for i, stmt in enumerate(statements, 1):
            if not stmt or len(stmt) < 10:
                continue
            
            try:
                # Parse with Redshift dialect
                parsed = parse_one(stmt, dialect='redshift')
                valid_count += 1
            except Exception as e:
                error_count += 1
                error_msg = f"Statement {i} validation error: {str(e)[:100]}"
                self.errors.append(error_msg)
                print(f"  ✗ {error_msg}")
        
        print(f"\nValidation Results:")
        print(f"  ✓ Valid statements: {valid_count}")
        print(f"  ✗ Invalid statements: {error_count}")
        
        return error_count == 0
        
    except Exception as e:
        self.errors.append(f"Validation error: {str(e)}")
        print(f"  ✗ Validation failed: {str(e)}")
        return False

def print_errors(self):
    """Print all accumulated errors"""
    if self.errors:
        print("\n" + "="*70)
        print("ERRORS ENCOUNTERED:")
        print("="*70)
        for i, error in enumerate(self.errors, 1):
            print(f"{i}. {error}")
    else:
        print("\n✓ No errors encountered")
```

def create_sample_excel_files():
“”“Create sample Excel files with the required schema”””

```
# Sample Tables Sheet
tables_data = {
    'schema_name': ['dwh', 'dwh', 'dwh'],
    'table_name': ['dim_customer', 'dim_product', 'fact_sales'],
    'description': [
        'Customer dimension table',
        'Product dimension table', 
        'Sales fact table'
    ],
    'primary_key': ['customer_key', 'product_key', 'sales_key'],
    'dist_style': ['KEY', 'ALL', 'KEY'],
    'dist_key': ['customer_key', None, 'customer_key'],
    'sort_keys': ['customer_id', 'product_id', 'sale_date, customer_key'],
    'sort_type': ['COMPOUND', 'COMPOUND', 'COMPOUND']
}

# Sample Columns Sheet
columns_data = {
    'schema_name': ['dwh', 'dwh', 'dwh', 'dwh', 'dwh', 'dwh', 'dwh', 'dwh', 'dwh', 'dwh'],
    'table_name': ['dim_customer', 'dim_customer', 'dim_customer', 'dim_customer', 'dim_customer',
                   'dim_product', 'dim_product', 'dim_product', 'dim_product', 'dim_product'],
    'column_name': ['customer_key', 'customer_id', 'customer_name', 'email', 'created_date',
                    'product_key', 'product_id', 'product_name', 'price', 'created_date'],
    'column_order': [1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
    'data_type': ['BIGINT', 'VARCHAR(50)', 'VARCHAR(200)', 'VARCHAR(200)', 'TIMESTAMP',
                  'BIGINT', 'VARCHAR(50)', 'VARCHAR(200)', 'DECIMAL(10,2)', 'TIMESTAMP'],
    'not_null': [True, True, True, False, True, True, True, True, True, True],
    'default_value': [None, None, None, None, 'GETDATE()', None, None, None, None, 'GETDATE()'],
    'encode': ['RAW', 'LZO', 'LZO', 'LZO', 'RAW', 'RAW', 'LZO', 'LZO', 'RAW', 'RAW']
}

mappings_data = {
    'target_schema': ['dwh', 'dwh', 'dwh', 'dwh'],
    'target_table': ['dim_customer', 'dim_customer', 'dim_customer', 'dim_customer'],
    'target_column': ['customer_key', 'customer_id', 'customer_name', 'email'],
    'target_column_order': [1, 2, 3, 4],
    'source_schema': ['staging', 'staging', 'staging', 'staging'],
    'source_table': ['stg_customers', 'stg_customers', 'stg_customers', 'stg_customers'],
    'source_column': [None, 'cust_id', 'name', 'email_address'],
    'transformation': ['ROW_NUMBER() OVER (ORDER BY cust_id)', None, 'UPPER(name)', None],
    'constant_value': [None, None, None, None],
    'is_business_key': [False, True, False, False],
    'scd_type': ['TYPE1', 'TYPE1', 'TYPE1', 'TYPE1']
}

# Create Excel files using Polars
pl.DataFrame(tables_data).write_excel('table_definitions.xlsx', worksheet='Tables')
pl.DataFrame(columns_data).write_excel('table_definitions.xlsx', worksheet='Columns')
pl.DataFrame(mappings_data).write_excel('source_target_mappings.xlsx', worksheet='Mappings')

console.print("✓ Sample Excel files created:", style="green")
console.print("  - table_definitions.xlsx")
console.print("  - source_target_mappings.xlsx")
```

# CLI Commands

@app.command()
def generate(
table_def: Path = typer.Option(
…, “–table-def”, “-t”,
help=“Path to table definitions Excel file”
),
mapping: Path = typer.Option(
…, “–mapping”, “-m”,
help=“Path to source-target mapping Excel file”
),
output_dir: Path = typer.Option(
Path(“output”), “–output”, “-o”,
help=“Output directory for generated files”
),
ddl: bool = typer.Option(
True, “–ddl/–no-ddl”,
help=“Generate DDL scripts”
),
dml: bool = typer.Option(
True, “–dml/–no-dml”,
help=“Generate DML scripts”
),
validate: bool = typer.Option(
True, “–validate/–no-validate”,
help=“Validate generated SQL”
)
):
“””
Generate DDL and DML SQL scripts from Excel definitions.

```
Example:
    python script.py generate -t tables.xlsx -m mappings.xlsx -o output/
"""
console.print("\n[bold cyan]AWS Redshift SQL Generator[/bold cyan]")
console.print("="*70)

# Create output directory
output_dir.mkdir(parents=True, exist_ok=True)

# Initialize generator
generator = RedshiftSQLGenerator()

# Load definitions
if not generator.load_definitions(table_def, mapping):
    raise typer.Exit(code=1)

# Generate DDL
if ddl:
    ddl_file = output_dir / 'ddl_scripts.sql'
    if not generator.generate_ddl(ddl_file):
        raise typer.Exit(code=1)
    
    if validate:
        validate_sql_file(ddl_file)

# Generate DML
if dml:
    dml_file = output_dir / 'dml_scripts.sql'
    if not generator.generate_dml(dml_file):
        raise typer.Exit(code=1)
    
    if validate:
        validate_sql_file(dml_file)

console.print("\n[bold green]✓ Generation Complete![/bold green]")
```

@app.command()
def validate(
sql_file: Path = typer.Argument(
…,
help=“SQL file to validate”
),
dialect: str = typer.Option(
“redshift”,
“–dialect”, “-d”,
help=“SQL dialect (redshift, postgres, mysql, etc.)”
)
):
“””
Validate SQL file syntax.

```
Example:
    python script.py validate ddl_scripts.sql
    python script.py validate -d postgres my_script.sql
"""
console.print(f"\n[bold cyan]SQL Validator[/bold cyan] (Dialect: {dialect})")
console.print("="*70)

if not sql_file.exists():
    console.print(f"✗ File not found: {sql_file}", style="red")
    raise typer.Exit(code=1)

success = validate_sql_file(sql_file)

if success:
    console.print("\n[bold green]✓ All SQL statements are valid![/bold green]")
else:
    console.print("\n[bold red]✗ Some SQL statements have errors[/bold red]")
    raise typer.Exit(code=1)
```

@app.command()
def document(
table_def: Path = typer.Option(
…, “–table-def”, “-t”,
help=“Path to table definitions Excel file”
),
mapping: Path = typer.Option(
None, “–mapping”, “-m”,
help=“Path to source-target mapping Excel file (for lineage)”
),
output: Path = typer.Option(
Path(“documentation”), “–output”, “-o”,
help=“Output file path”
),
format: DocumentFormat = typer.Option(
DocumentFormat.html, “–format”, “-f”,
help=“Output format: yaml, markdown, or html”
)
):
“””
Generate database documentation with lineage diagrams.

```
Examples:
    python script.py document -t tables.xlsx -f html -o docs.html
    python script.py document -t tables.xlsx -m mappings.xlsx -f markdown -o README.md
    python script.py document -t tables.xlsx -f yaml -o metadata.yaml
"""
console.print("\n[bold cyan]Documentation Generator[/bold cyan]")
console.print("="*70)

# Initialize generator
generator = RedshiftSQLGenerator()

# Load definitions
if not table_def.exists():
    console.print(f"✗ File not found: {table_def}", style="red")
    raise typer.Exit(code=1)

# For documentation, we need at least table definitions
# Mappings are optional (for lineage)
try:
    generator.tables_df = pl.read_excel(table_def, sheet_name='Tables')
    generator.columns_df = pl.read_excel(table_def, sheet_name='Columns')
    
    if mapping and mapping.exists():
        generator.mappings_df = pl.read_excel(mapping, sheet_name='Mappings')
        console.print("✓ Loaded tables, columns, and mappings", style="green")
    else:
        console.print("✓ Loaded tables and columns (no lineage data)", style="yellow")
except Exception as e:
    console.print(f"✗ Error loading files: {str(e)}", style="red")
    raise typer.Exit(code=1)

# Set output extension based on format
if output.suffix == '':
    if format == DocumentFormat.yaml:
        output = output.with_suffix('.yaml')
    elif format == DocumentFormat.markdown:
        output = output.with_suffix('.md')
    elif format == DocumentFormat.html:
        output = output.with_suffix('.html')

# Generate documentation
if not generator.generate_documentation(output, format):
    raise typer.Exit(code=1)

console.print(f"\n[bold green]✓ Documentation generated successfully![/bold green]")
console.print(f"[cyan]Format:[/cyan] {format.value}")
console.print(f"[cyan]Location:[/cyan] {output}")
```

@app.command()
def create_sample():
“””
Create sample Excel files with proper schema.

```
Example:
    python script.py create-sample
"""
console.print("\n[bold cyan]Creating Sample Files[/bold cyan]")
console.print("="*70)

create_sample_excel_files()

console.print("\n[bold green]✓ Sample files created![/bold green]")
console.print("\n[yellow]Next steps:[/yellow]")
console.print("  1. Edit the Excel files with your table definitions")
console.print("  2. Run: python script.py generate -t table_definitions.xlsx -m source_target_mappings.xlsx")
console.print("  3. Run: python script.py document -t table_definitions.xlsx -m source_target_mappings.xlsx -f html")
```

@app.command()
def info():
“””
Display information about the tool and Excel schema.
“””
console.print(”\n[bold cyan]AWS Redshift SQL Generator - Information[/bold cyan]”)
console.print(”=”*70)

```
console.print("\n[bold]Excel Schema:[/bold]")
console.print("\n[yellow]table_definitions.xlsx[/yellow] should contain:")

console.print("\n  [bold]Sheet: 'Tables'[/bold]")
console.print("    - schema_name: Schema name (e.g., 'dwh')")
console.print("    - table_name: Table name")
console.print("    - description: Table description")
console.print("    - primary_key: Primary key columns (comma-separated)")
console.print("    - dist_style: Distribution style (KEY, EVEN, ALL)")
console.print("    - dist_key: Distribution key column")
console.print("    - sort_keys: Sort key columns (comma-separated)")
console.print("    - sort_type: COMPOUND or INTERLEAVED")

console.print("\n  [bold]Sheet: 'Columns'[/bold]")
console.print("    - schema_name: Schema name")
console.print("    - table_name: Table name")
console.print("    - column_name: Column name")
console.print("    - column_order: Order in table")
console.print("    - data_type: Redshift data type")
console.print("    - not_null: Boolean for NOT NULL")
console.print("    - default_value: Default value expression")
console.print("    - encode: Encoding type (RAW, LZO, etc.)")

console.print("\n[yellow]source_target_mappings.xlsx[/yellow] should contain:")

console.print("\n  [bold]Sheet: 'Mappings'[/bold]")
console.print("    - target_schema/table/column: Target details")
console.print("    - target_column_order: Column order")
console.print("    - source_schema/table/column: Source details")
console.print("    - transformation: SQL transformation expression")
console.print("    - constant_value: Constant value")
console.print("    - is_business_key: Boolean for business key")
console.print("    - scd_type: TYPE1, TYPE2, or INSERT")

console.print("\n[bold]Features:[/bold]")
console.print("  ✓ DDL generation with Redshift optimizations")
console.print("  ✓ DML generation (SCD Type 1, Type 2, Insert)")
console.print("  ✓ SQL validation using SQLGlot")
console.print("  ✓ Documentation in YAML, Markdown, or HTML")
console.print("  ✓ Data lineage diagrams using Mermaid.js")
console.print("  ✓ Built with Polars, Jinja2, and Typer")

console.print("\n[bold]Example Commands:[/bold]")
console.print("  python script.py create-sample")
console.print("  python script.py generate -t tables.xlsx -m mappings.xlsx")
console.print("  python script.py validate ddl_scripts.sql")
console.print("  python script.py document -t tables.xlsx -f html -o docs.html")
```

if **name** == “**main**”:
app()
‘default_value’: [None, None, None, None, ‘GETDATE()’, None, None, None, None, ‘GETDATE()’],
‘encode’: [‘RAW’, ‘LZO’, ‘LZO’, ‘LZO’, ‘RAW’, ‘RAW’, ‘LZO’, ‘LZO’, ‘RAW’, ‘RAW’]
}

```
# Sample Mappings Sheet
mappings_data = {
    'target_schema': ['dwh', 'dwh', 'dwh', 'dwh'],
    'target_table': ['dim_customer', 'dim_customer', 'dim_customer', 'dim_customer'],
    'target_column': ['customer_key', 'customer_id', 'customer_name', 'email'],
    'target_column_order': [1, 2, 3, 4],
    'source_schema': ['staging', 'staging', 'staging', 'staging'],
    'source_table': ['stg_customers', 'stg_customers', 'stg_customers', 'stg_customers'],
    'source_column': [None, 'cust_id', 'name', 'email_address'],
    'transformation': ['ROW_NUMBER() OVER (ORDER BY cust_id)', None, 'UPPER(name)', None],
    'constant_value': [None, None, None, None],
    'is_business_key': [False, True, False, False],
    'scd_type': ['TYPE1', 'TYPE1', 'TYPE1', 'TYPE1']
}

# Create Excel files
with pd.ExcelWriter('table_definitions.xlsx', engine='openpyxl') as writer:
    pd.DataFrame(tables_data).to_excel(writer, sheet_name='Tables', index=False)
    pd.DataFrame(columns_data).to_excel(writer, sheet_name='Columns', index=False)

with pd.ExcelWriter('source_target_mappings.xlsx', engine='openpyxl') as writer:
    pd.DataFrame(mappings_data).to_excel(writer, sheet_name='Mappings', index=False)

print("✓ Sample Excel files created:")
print("  - table_definitions.xlsx")
print("  - source_target_mappings.xlsx")
```

# Main execution

if **name** == “**main**”:
print(”=”*70)
print(“AWS Redshift SQL Code Generator”)
print(”=”*70)

```
# Create sample files (remove this in production)
create_sample_excel_files()

# Initialize generator
generator = RedshiftSQLGenerator(
    table_def_file='table_definitions.xlsx',
    mapping_file='source_target_mappings.xlsx'
)

# Load definitions
if not generator.load_definitions():
    generator.print_errors()
    exit(1)

# Generate DDL
if not generator.generate_ddl('ddl_scripts.sql'):
    generator.print_errors()
    exit(1)

# Generate DML
if not generator.generate_dml('dml_scripts.sql'):
    generator.print_errors()
    exit(1)

# Validate generated SQL
print("\n" + "="*70)
generator.validate_sql('ddl_scripts.sql')
generator.validate_sql('dml_scripts.sql')

# Print any errors
generator.print_errors()

print("\n" + "="*70)
print("Generation Complete!")
print("="*70)
```