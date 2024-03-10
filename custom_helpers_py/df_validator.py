def validate_prod_sup_similarweb_df(df):
    columns = df.columns
    required_cols = ["source_domain", "total_visits_last_month", "data_created_at"]
    if len(columns) != len(required_cols):
        raise "prod similarweb df has mismatched length"

    for col_name in required_cols:
        if col_name not in columns:
            raise col_name + " not in prod similarweb df"

    return True
