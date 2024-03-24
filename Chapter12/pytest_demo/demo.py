import polars as pl

def add_ranks(df):
    return (
        df
        .with_columns(
            pl.col('Attack').rank(method='dense').alias('Atk Rank'),
            pl.col('Defense').rank(method='dense').alias('Def Rank'),
            pl.col('Speed').rank(method='dense').alias('Spe Rank'),
        )
    )

def keep_cols(df):
    return (
        df
        .select(
            'Name',
            'Type 1',
            'Type 2',
            'Total',
            'Attack',
            'Defense',
            'Speed',
            pl.col('^*Rank$')
        )
    )

def keep_grass_or_fire(df):
    accepted_types = ['Grass', 'Fire']
    return (
        df
        .filter(
            (pl.col('Type 1').is_in(accepted_types))
            | (pl.col('Type 2').is_in(accepted_types))
        )
    )
