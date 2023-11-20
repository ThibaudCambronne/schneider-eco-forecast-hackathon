import argparse
import pandas as pd
import os
import utils

def load_data(file_path):
    # TODO: Load data from CSV file

    return df

def clean_data(input_file): # df, 
    # TODO: Handle missing values, outliers, etc.
    # initialize the data frame
    psr_type_to_gen_type = utils.init_psr_type_to_gen_type()
    # initialize the data frame
    df_clean = pd.DataFrame()
    missing_values = 0
    interpolated_values = 0
    resampling_loss = 0
    total_number_of_data_points = 0

    # Iter throught all the files in the input folder
    for file in os.listdir(input_file):
        file_name = file.split(".")[0]
        country = file_name.split("_")[1]

        # get file type (load of gen type)
        try:
            data_type = psr_type_to_gen_type[file_name.split("_")[2]]
        except IndexError:
            data_type = "Load"
        
        new_column_name = country + "_" + data_type
        # print("# Cleaning: ", new_column_name)

        # read the csv file
        df = pd.read_csv(input_file + "/" + file)

        # drop rows where area is nan
        try:
            df = df.dropna(subset=["AreaID"])
        except KeyError:
            pass

        size_data = df.shape[0]
        total_number_of_data_points += size_data
        # Handle case when data frame is empty
        if size_data == 0:
            print("Empty data frame")
            # create a data frame filles with 0 with the same index as the other data frames
            if data_type == "Load":
                df = pd.DataFrame(columns=["quantity"], index=df_clean.index).fillna(1000000)
            else:
                df = pd.DataFrame(columns=["quantity"], index=df_clean.index).fillna(0)
            freq = "H"
        
        else:
            # Get the data frequency from the end and start time of the first row
            freq = pd.Timestamp(df["EndTime"].iloc[0].replace("Z", "")) - pd.Timestamp(df["StartTime"].iloc[0].replace("Z", ""))
            if freq == pd.Timedelta("0 days 00:15:00"):
                freq = "15T"
                nb_data_points_per_hour = 4
            elif freq == pd.Timedelta("0 days 00:30:00"):
                freq = "30T"
                nb_data_points_per_hour = 2
            elif freq == pd.Timedelta("0 days 01:00:00"):
                freq = "H"
                nb_data_points_per_hour = 1
            else:
                raise ValueError("Unknown frequency")

            # make sure that the unit is MAW
            if df["UnitName"].unique() != ["MAW"]:
                raise ValueError(f"Unknown {df['UnitName'].unique()} unit(s)")

            # Drop the columns that are not needed to save memory
            if data_type == "Load":
                df = df.drop(["EndTime", "AreaID", "UnitName"], axis=1)
                df = df.rename(columns={"Load": "quantity"})
            else:
                df = df.drop(["EndTime", "AreaID", "UnitName", "PsrType"], axis=1)

            # Convert StartTime to date format
            df["StartTime"] = df["StartTime"].apply(lambda x: pd.Timestamp(x.replace("Z", "")))

            # set index to StartTime
            df = df.set_index("StartTime")

            # Merge with empty data frame with all the desired time stamps, to make sure that there is no missing data
            df = pd.merge(df, pd.DataFrame(index=pd.date_range(start=df.index.min(), end=df.index.max(), freq=freq)), left_index=True, right_index=True, how="right")
        
        missing_values += df["quantity"].isna().sum()
        missing_values_this_df = df["quantity"].isna().sum()

        # Interpolate the missing values with the mean of the previous and next value
        # Limit the interpolation because we don't want to interpolate too many values in a row (max 3 hours)
        df = df.interpolate(method="linear", limit_direction="both", limit=3*nb_data_points_per_hour)
        interpolated_values += missing_values_this_df - df["quantity"].isna().sum()

        # if needed, resample the data to 1 hour by summing the 15min data
        if freq in ["15T", "30T"]:
            # resample data
            df_resampled = df.resample("1H").sum(numeric_only=True, min_count=nb_data_points_per_hour).dropna()

            # Merge with empty data frame with all the desired time stamps, to make sure that there is no missing data
            df_resampled = pd.merge(df_resampled, pd.DataFrame(index=pd.date_range(start=df_resampled.index.min(), end=df_resampled.index.max(), freq="1H")), left_index=True, right_index=True, how="right")

            # Compute the resampling loss by comparing the resampled dataFrame with the original dataFrame
            df_resampling_loss = pd.merge(df, df_resampled, left_index=True, right_index=True, how="left", suffixes=("", "_1H")).groupby(pd.Grouper(freq="1H")).count()
            df_resampling_loss["quantity_1H"] *= nb_data_points_per_hour
            resampling_loss += (df_resampling_loss["quantity"] - df_resampling_loss["quantity_1H"]).sum()
            df = df_resampled

        # rename quantity column with country and data type
        df = df.rename(columns={"quantity": new_column_name})

        # add the resampled data to the data frame
        df_clean = pd.merge(df_clean, df, left_index=True, right_index=True, how="outer")

        
        if data_type == "Load":
            # fillna for loads with large values so that an unknown load doesn't mean a good surplus
            pass
            # df_clean[new_column_name] = df_clean[new_column_name].fillna(10000000)
        else:
            df_clean[new_column_name] = df_clean[new_column_name].fillna(0)

    print("Missing raw values: ", round(missing_values/total_number_of_data_points*100, 2), "%")
    print("Ratio of interpolated raw values: ", round(interpolated_values/total_number_of_data_points*100, 2), "%")
    print("Hourly resampling loss: ", round(resampling_loss/total_number_of_data_points*100, 2), "%")

    return df_clean

def compute_biggest_surplus(row, df, dict_country_id):

    dict_total_consumption = {k: 0 for k in dict_country_id.keys()}
    columns = df.columns
    for country in list(dict_country_id.keys()):
        for column in columns:
            if (country in column):
                if ("Load" in column):
                    dict_total_consumption[country] -= row[column]
                else:
                    dict_total_consumption[country] += row[column] if not np.isnan(row[column]) else 0
    # get country with biggest surplus
    biggest_surplus_country = max(dict_total_consumption, key=dict_total_consumption.get)

    classification_list = [0]*len(dict_country_id)
    classification_list[dict_country_id[biggest_surplus_country]] = 1

    # print(dict_total_consumption, biggest_surplus_country)
    return dict_country_id[biggest_surplus_country]

def preprocess_data(df):
    # TODO: Generate new features, transform existing features, resampling, etc.
    dict_country_id = {
        "SP": 0, # Spain
        "UK": 1, # United Kingdom
        "DE": 2, # Germany
        "DK": 3, # Denmark
        "HU": 5, # Hungary
        "SE": 4, # Sweden
        "IT": 6, # Italy
        "PO": 7, # Poland
        "NE": 8 # Netherlands
        }
    df["Country_biggest_surplus"] = df.apply(compute_biggest_surplus, axis=1, args=(df, dict_country_id))

    return df

def save_data(df, output_file):
    df.to_csv(output_file)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Data processing script for Energy Forecasting Hackathon')
    parser.add_argument(
        '--input_file',
        type=str,
        default='data/raw',
        help='Path to the raw data file to process'
    )
    parser.add_argument(
        '--output_file', 
        type=str, 
        default='data/processed_data.csv', 
        help='Path to save the processed data'
    )
    return parser.parse_args()

def main(input_file, output_file):
    # df = load_data(input_file)
    df_clean = clean_data(input_file) # df, 
    df_processed = preprocess_data(df_clean)
    save_data(df_processed, output_file)

if __name__ == "__main__":
    args = parse_arguments()
    main(args.input_file, args.output_file)