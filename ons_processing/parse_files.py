"""
Functions for parsing ONS data into a format ready for analysis

Some preprocessing has already been done on the downloaded excel files to make the parsing easier.

FIXME: some repetition here between functions. Can fix eg by creating a single loading class/function
"""

import os

import pandas as pd


LA_CODE = "la_code"  # we will join all data on this key
LA_NAME = "la_name"  # for convenience


def load_df(df_dir, file_name, **read_kwargs):
    """Convenience function to read a CSV file into a df from a file in df_dir"""
    csv_path = os.path.join(df_dir, file_name)
    return pd.read_csv(csv_path, **read_kwargs)


def subset_rename_df(df, name_dict):
    """Subsets the given df on the keys of name_dict and renames these columns using the same dict"""
    return df[list(name_dict)].rename(columns=name_dict)


def parse_wellbeing(csv_dir):
    """Parse wellbeing estimates.
    https://download.ons.gov.uk/downloads/datasets/wellbeing-local-authority/editions/time-series/versions/2.csv
    """
    df = load_df(csv_dir, "wellbeing.csv")
    df = df.loc[(df["wellbeing-estimate"] == "average-mean") & (df.Time == "2020-21")]

    cols = {
        "V4_3": "wellbeing_score",
        "administrative-geography": LA_CODE,
        "measure-of-wellbeing": "wellbeing_measure",
    }
    sub_df = subset_rename_df(df, cols)
    return (
        sub_df.drop_duplicates(subset=["la_code", "wellbeing_measure"])
        .pivot(index="la_code", columns="wellbeing_measure", values="wellbeing_score")
        .reset_index()
    )


def parse_population_age(csv_dir):
    """Parse population estimates by age.
    https://www.ons.gov.uk/file?uri=%2fpeoplepopulationandcommunity%2fpopulationandmigration%2fpopulationestimates%2fdatasets%2fwardlevelmidyearpopulationestimatesexperimental%2fmid2020sape23dt8a/sape23dt8amid2020ward2020on2021lasyoaestimatesunformattedcorrection.xlsx
    First download the excel file and export the relevant part of the "Mid-2020 Persons" sheet as a CSV file to csv_path.
    """
    df = load_df(csv_dir, "population_by_age.csv")

    age_cols = {
        "child": [str(k) for k in range(19)],
        "adult": [str(k) for k in range(19, 65)],
        "elderly": [str(k) for k in range(65, 90)] + ["90+"],
    }  # aggregate into population groups now for later convenience
    age_labels = list(age_cols)

    for group, col_subset in age_cols.items():
        df.loc[:, group] = df[col_subset].sum(axis=1)

    cols = {
        "LA Code (2021 boundaries)": LA_CODE,
        "LA name (2021 boundaries)": LA_NAME,
        "Ward Code 1": "ward_code",
        "Ward Name 1": "ward_name",
        "All Ages": "total_population",
        **dict(zip(age_labels, age_labels)),
    }
    sub_df = subset_rename_df(df, cols)
    return (
        sub_df.groupby(["la_code", "la_name"])[
            ["total_population", "child", "adult", "elderly"]
        ]
        .sum()
        .reset_index()
    )


def parse_rental_summary(csv_dir):
    """Parse rental market summary statistic
    https://www.ons.gov.uk/file?uri=%2fpeoplepopulationandcommunity%2fhousing%2fdatasets%2fprivaterentalmarketsummarystatisticsinengland%2fapril2020tomarch2021/privaterentalmarketstatistics210616.xls
    Export the relevant parts of sheet "Table2.7" (summary of all rentals) to a CSV file
    """
    df = load_df(csv_dir, "rental.csv")
    cols = {
        "Area Code1": LA_CODE,
        "Count of rents": "rent_count",
        "Median": "median_rent",
        "Mean": "mean_rent",
    }
    # When "LA Code1" is NaN, that entry is an aggregate statistic, eg for England
    return subset_rename_df(df, cols).dropna(subset=[LA_CODE])


def parse_crime(csv_dir):
    """Parse crime statistics
    https://www.ons.gov.uk/file?uri=%2fpeoplepopulationandcommunity%2fcrimeandjustice%2fdatasets%2frecordedcrimedatabycommunitysafetypartnershiparea%2fyearendingjune2021/csptablejune21final.xlsx
    Export the relevant parts of sheet "Table C5" (per capita crime) to a CSV file
    """
    df = load_df(csv_dir, "crime.csv")
    cols = {
        "Local Authority code": LA_CODE,
        "Household figures (mid-2019) - rounded to 100": "num_households",
        "Total recorded crime\n (excluding fraud)": "total_crime",
        "Residential burglary (per 1,000 household)": "burgalry_per_household",
    }
    return subset_rename_df(df, cols).dropna(subset=[LA_CODE])


def parse_property_sales(csv_dir):
    """Parse property sales statistics
    https://www.ons.gov.uk/file?uri=%2fpeoplepopulationandcommunity%2fhousing%2fdatasets%2fnumberofresidentialpropertysalesbywardhpssadataset36%2fyearendingjune2021/hpssadataset36numberofresidentialpropertysalesbyward1.zip
    Export the relevant parts of sheet "1a" (total house sales) to a CSV file
    """
    df = load_df(csv_dir, "property_sales.csv")
    cols = {
        "Local authority code": LA_CODE,
        "Ward code": "ward_code",
        "Year ending Jun 2021": "num_sold",
    }
    sub_df = subset_rename_df(df, cols).dropna(subset=[LA_CODE])
    return (
        sub_df.groupby(LA_CODE)["num_sold"].sum().reset_index()
    )  # aggregate over la codes


def parse_property_prices(csv_dir):
    """Parse median property sale statistics
    https://www.ons.gov.uk/file?uri=%2fpeoplepopulationandcommunity%2fhousing%2fdatasets%2fmedianhousepricefornationalandsubnationalgeographiesexistingdwellingsquarterlyrollingyearhpssadataset11%2fcurrent/hpssadataset11medianpricepaidforadministrativegeographiesexistingdwellings1.xls
    Export the relevant parts of sheet "1a" (median house price) to a CSV file
    """
    df = load_df(csv_dir, "house_prices.csv")
    cols = {
        "Local authority code ": LA_CODE,
        "Year ending Jun 2021": "property_price",
    }
    return subset_rename_df(df, cols).dropna(subset=[LA_CODE])


def parse_earnings_to_house_price(csv_dir):
    """Parse earnings / house price statistics
    https://www.ons.gov.uk/peoplepopulationandcommunity/housing/datasets/ratioofhousepricetoworkplacebasedearningslowerquartileandmedian
    Export the relevant parts of sheet "5c" (ratio of median house price to median earnings) to a CSV file
    """
    df = load_df(csv_dir, "earnings_price_ratio.csv")
    cols = {
        "Code": LA_CODE,
        "2020": "earnings_house_price_ratio",
    }
    return subset_rename_df(df, cols).dropna(subset=[LA_CODE])
