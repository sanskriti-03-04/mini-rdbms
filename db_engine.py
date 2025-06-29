import re
import pandas as pd

def execute_query(query, tables):
    try:
        query = query.strip().lower()

        # COUNT or AVG
        agg_match = re.match(r"select (count|avg)\((\*|\w+)\) from (\w+)", query)
        if agg_match:
            func, col, table_name = agg_match.groups()
            if table_name not in tables:
                return f"Table '{table_name}' not found."
            df = tables[table_name]
            if func == "count":
                return pd.DataFrame({"count": [len(df)]})
            elif func == "avg":
                if col not in df.columns:
                    return f"Column '{col}' not found."
                avg = pd.to_numeric(df[col], errors="coerce").mean()
                return pd.DataFrame({f"avg_{col}": [round(avg, 2)]})

        # JOIN support (basic inner join)
        join_match = re.match(r"select \* from (\w+) join (\w+) on (\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", query)
        if join_match:
            t1, t2, a1, c1, a2, c2 = join_match.groups()
            if t1 in tables and t2 in tables:
                df1 = tables[t1]
                df2 = tables[t2]
                joined = pd.merge(df1, df2, left_on=c1, right_on=c2)
                return joined
            else:
                return f"One or both tables '{t1}', '{t2}' not found."

        # SELECT with WHERE and column selection
        select_pattern = r"select\s+([\*\w, ]+)\s+from\s+(\w+)(?:\s+where\s+(\w+)\s*(=|>|<)\s*('?[\w\d\s]+'?))?"
        match = re.match(select_pattern, query)
        if match:
            cols, table_name, where_col, op, where_val = match.groups()
            if table_name not in tables:
                return f"Table '{table_name}' not found."
            df = tables[table_name]
            if cols.strip() == "*":
                selected_df = df.copy()
            else:
                col_list = [c.strip() for c in cols.split(",")]
                selected_df = df[col_list]

            if where_col:
                where_val = where_val.strip("'")
                if op == "=":
                    selected_df = selected_df[selected_df[where_col] == where_val]
                elif op == ">":
                    selected_df = selected_df[pd.to_numeric(selected_df[where_col], errors='coerce') > float(where_val)]
                elif op == "<":
                    selected_df = selected_df[pd.to_numeric(selected_df[where_col], errors='coerce') < float(where_val)]

            return selected_df.reset_index(drop=True)

        # INSERT
        match = re.match(r"insert into (\w+) values \((.+)\)", query)
        if match:
            table_name, values_raw = match.group(1), match.group(2)
            values = [v.strip().strip("'") for v in values_raw.split(',')]
            if table_name in tables:
                df = tables[table_name]
                if len(values) != len(df.columns):
                    return f"Expected {len(df.columns)} values, got {len(values)}"
                tables[table_name].loc[len(df)] = values
                return df
            else:
                return f"Table '{table_name}' not found."

        # DELETE
        match = re.match(r"delete from (\w+) where (\w+)=('?\w+'?)", query)
        if match:
            table_name, col, val = match.group(1), match.group(2), match.group(3).strip("'")
            if table_name in tables:
                df = tables[table_name]
                before = len(df)
                df = df[df[col] != val]
                after = len(df)
                tables[table_name] = df.reset_index(drop=True)
                return f"Deleted {before - after} row(s)."
            else:
                return f"Table '{table_name}' not found."

        # UPDATE
        update_pattern = r"update (\w+) set (\w+)=('?[\w\d\s]+'?) where (\w+)=('?[\w\d\s]+'?)"
        match = re.match(update_pattern, query)
        if match:
            table_name, set_col, set_val, where_col, where_val = match.groups()
            set_val = set_val.strip("'")
            where_val = where_val.strip("'")
            if table_name in tables:
                df = tables[table_name]
                updated_rows = df[where_col] == where_val
                count = updated_rows.sum()
                df.loc[updated_rows, set_col] = set_val
                return f"Updated {count} row(s)."
            else:
                return f"Table '{table_name}' not found."

        return "Unsupported or malformed query."

    except Exception as e:
        return f"Error: {e}"
