import pytest
import polars as pl
from polars.testing import assert_frame_equal
from cuallee import Check, CheckLevel
from demo import keep_grass_or_fire, keep_cols, add_ranks

@pytest.fixture(scope='session')
def df():
    df = (
        pl.read_csv('../../data/pokemon.csv')
        .pipe(keep_grass_or_fire)
        .pipe(keep_cols)
        .pipe(add_ranks)
    )
    return df

def test_selected_cols(df):
    cols = [
        'Name',
        'Type 1',
        'Type 2',
        'Total',
        'Attack',
        'Defense',
        'Speed',
        'Atk Rank',
        'Def Rank',
        'Spe Rank'
    ]
    assert sorted(df.columns) == sorted(cols)

def test_grass_or_fire_type(df):
    accepted_types = ['Grass', 'Fire']
    filtered_df = (
        df
        .filter(
            (pl.col('Type 1').is_in(accepted_types))
            | (pl.col('Type 2').is_in(accepted_types))
        )
    )
    assert filtered_df.height == df.height

def test_rank_range(df):
    bool_df = (
        df
        .select(
            pl.col('^*Rank$').is_between(1, df.height)
        )
        .select(
            pl.all_horizontal(pl.all())
        )
    )
    assert bool_df[0,0] == True

def test_ranks_completeness(df):
    check = Check(CheckLevel.WARNING, 'Completeness')
    cols = ['Atk Rank', 'Def Rank', 'Spe Rank']
    res_df = (
        check
        .are_complete(cols)
        .are_unique(cols)
        .validate(df)
    )
    assert res_df.select('status')[0,0]=='PASS'

def test_df_equal(df):
    result_df = pl.read_csv('../../data/pytest_expected_output.csv')
    return assert_frame_equal(df, result_df, check_dtype=False)
