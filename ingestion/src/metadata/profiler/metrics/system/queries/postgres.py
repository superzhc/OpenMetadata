
STAT_TABLE="""
select n_tup_ins,n_tup_upd,n_tup_del from pg_catalog.pg_stat_all_tables where schemaname='{schema}' and relname ='{table}'
"""