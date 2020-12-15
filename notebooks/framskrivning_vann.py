import os

import numpy as np
import pandas as pd
import teotil2 as teo


def get_annual_agricultural_coefficients(xl_path, sheet_name, core_fold):
    """Get annual agricultural inputs from NIBIO and convert to land use coefficients.
       Modified to read data directly from an Excel so it can be used for future land
       use scenarios.

    Args:
        xl_path:    Str. Path to Excel file with land cover data
        sheet_name: Str. Name of Excel worksheet
        core_fold:  Str. Path to folder containing core TEOTIL2 data files
    Returns:
        Dataframe
    """
    # Read LU areas (same values used every year)
    csv_path = os.path.join(core_fold, "fysone_land_areas.csv")
    lu_areas = pd.read_csv(csv_path, sep=";", encoding="windows-1252")

    # Read NIBIO data
    lu_lds = pd.read_excel(xl_path, sheet_name=sheet_name)

    # Join
    lu_df = pd.merge(lu_lds, lu_areas, how="outer", on="omrade")

    # Calculate required columns
    # N
    lu_df["agri_diff_tot-n_kg/km2"] = lu_df["n_diff_kg"] / lu_df["a_fy_agri_km2"]
    lu_df["agri_point_tot-n_kg/km2"] = (
        lu_df["n_point_kg"] / lu_df["a_fy_agri_km2"]
    )  # Orig a_fy_eng_km2??
    lu_df["agri_back_tot-n_kg/km2"] = lu_df["n_back_kg"] / lu_df["a_fy_agri_km2"]

    # P
    lu_df["agri_diff_tot-p_kg/km2"] = lu_df["p_diff_kg"] / lu_df["a_fy_agri_km2"]
    lu_df["agri_point_tot-p_kg/km2"] = (
        lu_df["p_point_kg"] / lu_df["a_fy_agri_km2"]
    )  # Orig a_fy_eng_km2??
    lu_df["agri_back_tot-p_kg/km2"] = lu_df["p_back_kg"] / lu_df["a_fy_agri_km2"]

    # Get cols of interest
    cols = [
        "fylke_sone",
        "fysone_name",
        "agri_diff_tot-n_kg/km2",
        "agri_point_tot-n_kg/km2",
        "agri_back_tot-n_kg/km2",
        "agri_diff_tot-p_kg/km2",
        "agri_point_tot-p_kg/km2",
        "agri_back_tot-p_kg/km2",
        "a_fy_agri_km2",
        "a_fy_eng_km2",
    ]

    lu_df = lu_df[cols]

    return lu_df


def make_rid_input_file(
    year, engine, core_fold, out_csv, xl_path, sheet_name, par_list=["Tot-N", "Tot-P"]
):
    """Builds a TEOTIL2 input file for the RID programme for the specified year.
       Modified to read agricultural coefficients from an Excel file, rather than
       from the database. This means the model can be run easily with different
       land use scenarios that are not in the database.

    Args:
        year:       Int. Year of interest
        par_list:   List. Parameters defined in
                    RESA2.RID_PUNKTKILDER_OUTPAR_DEF
        out_csv:    Str. Path for output CSV file
        core_fold:  Str. Path to folder containing core TEOTIL2 data files
        engine:     SQL-Alchemy 'engine' object already connected
                    to RESA2
        xl_path:    Str. Path to Excel file with NIBIO land cover data
        sheet_name: Str. Name of Excel worksheet in NIBIO file
    Returns:
        Dataframe. The CSV is written to the specified path.
    """

    # Read data from RESA2
    spr_df = teo.io.get_annual_spredt_data(year, engine, par_list=par_list)
    aqu_df = teo.io.get_annual_aquaculture_data(year, engine, par_list=par_list)
    ren_df = teo.io.get_annual_renseanlegg_data(year, engine, par_list=par_list)
    ind_df = teo.io.get_annual_industry_data(year, engine, par_list=par_list)
    q_df = teo.io.get_annual_vassdrag_mean_flows(year, engine)

    # Use modified function above to read agri data
    agri_df = get_annual_agricultural_coefficients(xl_path, sheet_name, core_fold)

    # Read core TEOTIL2 inputs
    # 1. Regine network
    # Changes to kommuner boundaries in 2017 require different files for
    # different years
    if year < 2017:
        csv_path = os.path.join(core_fold, "regine_pre_2017.csv")
        reg_df = pd.read_csv(csv_path, index_col=0, sep=";")
    elif year == 2017:
        csv_path = os.path.join(core_fold, "regine_2017.csv")
        reg_df = pd.read_csv(csv_path, index_col=0, sep=";")
    else:
        csv_path = os.path.join(core_fold, "regine_2018_onwards.csv")
        reg_df = pd.read_csv(csv_path, index_col=0, sep=";")

    # 2. Retention factors
    csv_path = os.path.join(core_fold, "retention_nutrients.csv")
    ret_df = pd.read_csv(csv_path, sep=";")

    # 3. Land cover
    csv_path = os.path.join(core_fold, "land_cover.csv")
    lc_df = pd.read_csv(csv_path, index_col=0, sep=";")

    # 4. Lake areas
    csv_path = os.path.join(core_fold, "lake_areas.csv")
    la_df = pd.read_csv(csv_path, index_col=0, sep=";")

    # 5. Background coefficients
    csv_path = os.path.join(core_fold, "back_coeffs.csv")
    back_df = pd.read_csv(csv_path, sep=";")

    # 7. Fylke-Sone
    csv_path = os.path.join(core_fold, "regine_fysone.csv")
    fy_df = pd.read_csv(csv_path, sep=";")

    # Convert par_list to lower case
    par_list = [i.lower() for i in par_list]

    # Process data
    # 1. Land use
    # 1.1 Land areas
    # Join lu datasets
    area_df = pd.concat([reg_df, lc_df, la_df], axis=1, sort=True)
    area_df.index.name = "regine"
    area_df.reset_index(inplace=True)

    # Fill NaN
    area_df.fillna(value=0, inplace=True)

    # Get total area of categories
    area_df["a_sum"] = (
        area_df["a_wood_km2"]
        + area_df["a_agri_km2"]
        + area_df["a_upland_km2"]
        + area_df["a_glacier_km2"]
        + area_df["a_urban_km2"]
        + area_df["a_sea_km2"]
        + area_df["a_lake_km2"]
    )

    # If total exceeds overall area, calc correction factor
    area_df["a_cor_fac"] = np.where(
        area_df["a_sum"] > area_df["a_reg_km2"],
        area_df["a_reg_km2"] / area_df["a_sum"],
        1,
    )

    # Apply correction factor
    area_cols = [
        "a_wood_km2",
        "a_agri_km2",
        "a_upland_km2",
        "a_glacier_km2",
        "a_urban_km2",
        "a_sea_km2",
        "a_lake_km2",
        "a_sum",
    ]

    for col in area_cols:
        area_df[col] = area_df[col] * area_df["a_cor_fac"]

    # Calc 'other' column
    area_df["a_other_km2"] = area_df["a_reg_km2"] - area_df["a_sum"]

    # Combine 'glacier' and 'upland' as 'upland'
    area_df["a_upland_km2"] = area_df["a_upland_km2"] + area_df["a_glacier_km2"]

    # Add 'land area' column
    area_df["a_land_km2"] = area_df["a_reg_km2"] - area_df["a_sea_km2"]

    # Tidy
    del area_df["a_glacier_km2"], area_df["a_sum"], area_df["a_cor_fac"]

    # 1.2. Join background coeffs
    area_df = pd.merge(area_df, back_df, how="left", on="regine")

    # 1.3. Join agri coeffs
    area_df = pd.merge(area_df, fy_df, how="left", on="regine")
    area_df = pd.merge(area_df, agri_df, how="left", on="fylke_sone")

    # 2. Discharge
    # Sum LTA to vassom level
    lta_df = area_df[["vassom", "q_reg_m3/s"]].groupby("vassom").sum().reset_index()
    lta_df.columns = ["vassom", "q_lta_m3/s"]

    # Join
    q_df = pd.merge(lta_df, q_df, how="left", on="vassom")

    # Calculate corr fac
    q_df["q_fac"] = q_df["q_yr_m3/s"] / q_df["q_lta_m3/s"]

    # Join and reset index
    df = pd.merge(area_df, q_df, how="left", on="vassom")
    df.index = df["regine"]
    del df["regine"]

    # Calculate regine-specific flow for this year
    for col in ["q_sp_m3/s/km2", "runoff_mm/yr", "q_reg_m3/s"]:
        df[col] = df[col] * df["q_fac"]

        # Fill NaN
        df[col].fillna(value=0, inplace=True)

    # Tidy
    del df["q_fac"], df["q_yr_m3/s"], df["q_lta_m3/s"]

    # 3. Point sources
    # 3.1. Aqu, ren, ind
    # List of data to concat later
    df_list = [
        df,
    ]

    # Set indices
    for pt_df in [aqu_df, ren_df, ind_df]:
        if pt_df is not None:
            pt_df.index = pt_df["regine"]
            del pt_df["regine"]
            df_list.append(pt_df)

    # Join
    df = pd.concat(df_list, axis=1, sort=True)
    df.index.name = "regine"
    df.reset_index(inplace=True)

    # Fill NaN
    for typ in ["aqu", "ren", "ind"]:
        for par in par_list:
            col = "%s_%s_tonnes" % (typ, par)
            if col in df.columns:
                df[col].fillna(value=0, inplace=True)
            else:  # Create cols of zeros
                df[col] = 0

    # 3.2. Spr
    # Get total land area and area of cultivated land in each kommune
    kom_df = df[["komnr", "a_land_km2", "a_agri_km2"]]
    kom_df = kom_df.groupby("komnr").sum()
    kom_df.reset_index(inplace=True)
    kom_df.columns = ["komnr", "a_kom_km2", "a_agri_kom_km2"]

    if spr_df is not None:
        # Join 'spredt' to kommune areas
        kom_df = pd.merge(kom_df, spr_df, how="left", on="komnr")

    else:  # Create cols of zeros
        for par in par_list:
            kom_df["spr_%s_tonnes" % par.lower()] = 0

    # Join back to main df
    df = pd.merge(df, kom_df, how="left", on="komnr")

    # Distribute loads
    for par in par_list:
        # Over agri
        df["spr_agri"] = (
            df["spr_%s_tonnes" % par] * df["a_agri_km2"] / df["a_agri_kom_km2"]
        )

        # Over all area
        df["spr_all"] = df["spr_%s_tonnes" % par] * df["a_land_km2"] / df["a_kom_km2"]

        # Use agri if > 0, else all
        df["spr_%s_tonnes" % par] = np.where(
            df["a_agri_kom_km2"] > 0, df["spr_agri"], df["spr_all"]
        )

    # Delete intermediate cols
    del df["spr_agri"], df["spr_all"]

    # Fill NaN
    df["a_kom_km2"].fillna(value=0, inplace=True)
    df["a_agri_kom_km2"].fillna(value=0, inplace=True)

    for par in par_list:
        # Fill
        df["spr_%s_tonnes" % par].fillna(value=0, inplace=True)

    # 4. Diffuse
    # Loop over pars
    for par in par_list:
        # Background inputs
        # Woodland
        df["wood_%s_tonnes" % par] = (
            df["a_wood_km2"]
            * df["q_sp_m3/s/km2"]
            * df["c_wood_mg/l_%s" % par]
            * 0.0864
            * 365
        )

        # Upland
        df["upland_%s_tonnes" % par] = (
            df["a_upland_km2"]
            * df["q_sp_m3/s/km2"]
            * df["c_upland_mg/l_%s" % par]
            * 0.0864
            * 365
        )

        # Lake
        df["lake_%s_tonnes" % par] = (
            df["a_lake_km2"] * df["c_lake_kg/km2_%s" % par] / 1000
        )

        # Urban
        df["urban_%s_tonnes" % par] = (
            df["a_urban_km2"] * df["c_urban_kg/km2_%s" % par] / 1000
        )

        # Agri from Bioforsk
        # Background
        df["agri_back_%s_tonnes" % par] = (
            df["a_agri_km2"] * df["agri_back_%s_kg/km2" % par] / 1000
        )

        # Point
        df["agri_pt_%s_tonnes" % par] = (
            df["a_agri_km2"] * df["agri_point_%s_kg/km2" % par] / 1000
        )

        # Diffuse
        df["agri_diff_%s_tonnes" % par] = (
            df["a_agri_km2"] * df["agri_diff_%s_kg/km2" % par] / 1000
        )

    # 5. Retention and transmission
    # Join
    df = pd.merge(df, ret_df, how="left", on="regine")

    # Fill NaN
    for par in par_list:
        # Fill NaN
        df["ret_%s" % par].fillna(value=0, inplace=True)

        # Calculate transmission
        df["trans_%s" % par] = 1 - df["ret_%s" % par]

    # 6. Aggregate values
    # Loop over pars
    for par in par_list:
        # All point sources
        df["all_point_%s_tonnes" % par] = (
            df["spr_%s_tonnes" % par]
            + df["aqu_%s_tonnes" % par]
            + df["ren_%s_tonnes" % par]
            + df["ind_%s_tonnes" % par]
            + df["agri_pt_%s_tonnes" % par]
        )

        # Natural diffuse sources
        df["nat_diff_%s_tonnes" % par] = (
            df["wood_%s_tonnes" % par]
            + df["upland_%s_tonnes" % par]
            + df["lake_%s_tonnes" % par]
            + df["agri_back_%s_tonnes" % par]
        )

        # Anthropogenic diffuse sources
        df["anth_diff_%s_tonnes" % par] = (
            df["urban_%s_tonnes" % par] + df["agri_diff_%s_tonnes" % par]
        )

        # All sources
        df["all_sources_%s_tonnes" % par] = (
            df["all_point_%s_tonnes" % par]
            + df["nat_diff_%s_tonnes" % par]
            + df["anth_diff_%s_tonnes" % par]
        )

    # 7. Lake volume
    # Estimate volume using poor relation from TEOTIL1
    df["mean_lake_depth_m"] = 1.8 * df["a_lake_km2"] + 13
    df["vol_lake_m3"] = df["mean_lake_depth_m"] * df["a_lake_km2"] * 1e6

    # Get cols of interest
    # Basic_cols
    col_list = [
        "regine",
        "regine_ned",
        "a_reg_km2",
        "runoff_mm/yr",
        "q_reg_m3/s",
        "vol_lake_m3",
    ]

    # Param specific cols
    #    par_cols = ['trans_%s', 'aqu_%s_tonnes', 'ind_%s_tonnes', 'ren_%s_tonnes',
    #                'spr_%s_tonnes', 'all_point_%s_tonnes', 'nat_diff_%s_tonnes',
    #                'anth_diff_%s_tonnes', 'all_sources_%s_tonnes']

    # Changed 21/11/2018. See e-mail from John Rune received 20/11/2018 at 16.15
    # Now include 'urban' and 'agri_diff' as separate categories
    par_cols = [
        "trans_%s",
        "aqu_%s_tonnes",
        "ind_%s_tonnes",
        "ren_%s_tonnes",
        "spr_%s_tonnes",
        "agri_pt_%s_tonnes",
        "all_point_%s_tonnes",
        "urban_%s_tonnes",
        "agri_diff_%s_tonnes",
        "nat_diff_%s_tonnes",
        "anth_diff_%s_tonnes",
        "all_sources_%s_tonnes",
    ]

    # Build col list
    for name in par_cols:
        for par in par_list:
            # Get col
            col_list.append(name % par)

    # Get cols
    df = df[col_list]

    # Remove rows where regine_ned is null
    df = df.query("regine_ned == regine_ned")

    # Fill Nan
    df.fillna(value=0, inplace=True)

    # 7. Write output
    df.to_csv(out_csv, encoding="utf-8", index=False)

    return df
