from intervals_api import API

from dropbox import DropboxOAuth2FlowNoRedirect, Dropbox

import configparser
import argparse
import logging

import json

import sys
import os

import pandas as pd
import numpy as np

from functools import partial
from itertools import product

from typing import Set
import hashlib

# Constants
APPLICATION_NAME = "HRV4Intervals"

# XDG Home Directories
CONFIG_DIR = os.path.expanduser(f"~/.config/{APPLICATION_NAME}")
CACHE_DIR = os.path.expanduser(f"~/.cache/{APPLICATION_NAME}")
STORAGE_DIR = os.path.expanduser(f"~/.local/share/{APPLICATION_NAME}")

APP_CONFIG_FILE_PATH = os.path.join(CONFIG_DIR, "HRV4Intervals.conf")

USER_CONFIG_FILE_PATH = os.path.join(CONFIG_DIR, "users.conf")
USER_CONFIG_ATHLETE_ID_FIELD = "intervals_user_id"
USER_CONFIG_API_KEY_FIELD = "intervals_api_key"

LOCAL_HRV_FILE_PATH_FORMAT = os.path.join(STORAGE_DIR, "{user}.csv")
REMOTE_HRV_FILE_PATH = "/Apps/HRV4TRAINING/MyMeasurements_Android.csv"

DROPBOX_TOKEN_PATH_FORMAT = os.path.join(STORAGE_DIR, "dropbox-tokens-{user}.csv")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--verbose", "-v", action="count", default=0)

    subparsers = parser.add_subparsers(title="Commands")

    add_account_command = subparsers.add_parser("add_account")
    add_account_command.set_defaults(func=add_account)
    add_account_command.add_argument("username")
    add_account_command.add_argument("--intervals-athlete-id")

    run_command = subparsers.add_parser("run")
    run_command.set_defaults(func=run)

    args = parser.parse_args()

    if "func" not in args:
        parser.print_help()
        sys.exit(1)

    if args.debug or args.verbose >= 2:
        logging.basicConfig(level=logging.DEBUG)
    elif args.verbose >= 1:
        logging.basicConfig(level=logging.INFO)

    config = parse_config(APP_CONFIG_FILE_PATH)

    args.func(args, config)


def add_account(args, config):
    user_config = parse_config(USER_CONFIG_FILE_PATH)

    if args.username in user_config.sections():
        print(f"Username {args.username} already exists")
        sys.exit(1)

    if args.intervals_athlete_id is not None:
        athlete_id = args.intervals_athlete_id
    else:
        athlete_id = input("Enter Intervals Athlete ID: ")

    api_key = input("Enter intervals API key: ")

    assert API.validate_athlete_id(athlete_id)
    assert API.validate_api_key(api_key)

    user_config.add_section(args.username)
    user_config.set(args.username, USER_CONFIG_ATHLETE_ID_FIELD, athlete_id)
    user_config.set(args.username, USER_CONFIG_API_KEY_FIELD, api_key)

    with open(USER_CONFIG_FILE_PATH, "w") as user_config_file:
        user_config.write(user_config_file)


def run(args, config):
    ensure_directory(CONFIG_DIR)
    ensure_directory(STORAGE_DIR)

    user_config = parse_config(USER_CONFIG_FILE_PATH)

    users = user_config.sections()

    if len(users) == 0:
        print("No users found. Check --help how to add an account.")

    for user in users:
        user_info = user_config[user]
        user_id = user_info[USER_CONFIG_ATHLETE_ID_FIELD]
        api_key = user_info[USER_CONFIG_API_KEY_FIELD]

        try:
            sync(user, user_id, api_key, config)
        except:
            logging.warning(f"Something went wrong for user: {user}, skipping")


def sync(user, athlete_id, api_key, config):
    download_path = LOCAL_HRV_FILE_PATH_FORMAT.format(user=user)

    dbx = get_dropbox_instance(user, config['Dropbox']['app_key'], config['Dropbox']['app_secret'])
    old_hash = get_md5sum(download_path) if os.path.exists(download_path) else None

    result = dbx.files_download_to_file(download_path, REMOTE_HRV_FILE_PATH)
    new_hash = get_md5sum(download_path)

    if old_hash == new_hash:
        logging.info(f"HRV4Training CSV did not change for user: {user}, skipping")
        return
    else:
        logging.info(f"Found new data from HRV4Training")
        logging.debug(f"Old hash: {old_hash}; new hash: {new_hash}")

    HRV4Training_data = pd.read_csv(download_path, index_col=False)

    intervals_data = parse_dataframe_HRV_to_intervals(HRV4Training_data)

    logging.info(f"Uploading {len(intervals_data)} entries to Intervals.icu for user: {user}")

    result = API(athlete_id, api_key).wellness_csv.update(intervals_data, index_label="date")

    assert 'status' not in result or result['status'] == 200


def parse_dataframe_HRV_to_intervals(data: pd.DataFrame) -> pd.DataFrame:

    hrv_to_intervals_map = {
        'rMSSD': ('hrv', None),
        'SDNN': ('hrvSDNN', None),
        'muscle_soreness': ('soreness', map_series),
        'fatigue': ('fatigue', map_series),
        'Stress': ('stress', map_series),
        'Mood': ('mood', map_series_reverse),
        'trainingMotivation': ('motivation', map_series_reverse),
        'sleep_quality': ('sleepQuality', map_series_reverse),
    }

    # HRV4Intervals provides a csv with spaces around the name for readability
    data = data.rename(columns=lambda x: x.strip())

    # HRV4Intervals provides a couple of rows that are completely empty.
    # Time is a good way to filter them out as it's not an optional field
    data = data[data.time.notna()]

    # Convert date from freedom format to ISO
    data['date'] = map_series_american_to_iso_date(data['date'])

    # Date is a good index for this data
    data = data.set_index('date')

    # HRV4Training allows up to three custom fields for the questionnaire after a measurement
    # Per entry we have a name of the question and coupled value.
    # We want to parse this so they are normal columns like all other questions

    additional_columns: Set[str] = set()

    # This seems needlessly complex, but this does account for when
    # somebody changing the order of their additional columns.
    for i in range(1, 4):
        additional_columns = additional_columns | set(data[f'custom_tag_{i}_name'].dropna())

    for column in additional_columns:
        column_data = pd.Series(dtype=float)

        for i in range(1, 4):
            indices = data[f'custom_tag_{i}_name'] == column
            column_data = column_data.combine_first(data[indices][f'custom_tag_{i}_value'].dropna())

        data[column] = column_data

    # Build new dataframe with new column names and after all values are mapped to their new scale
    new_data = pd.DataFrame()
    for old_name, (new_name, map_func) in hrv_to_intervals_map.items():
        new_data[new_name] = (
            data[old_name]
            if map_func is None else
            map_func(data[old_name])
        )

    return new_data


def parse_config(config_location: str):
    config = configparser.ConfigParser()
    config.read(config_location)
    return config


def ensure_directory(directory_path: str):
    """Ensure needed directories exist."""
    if not os.path.exists(directory_path):
        logging.info(f'Directory: "{directory_path}" does not exist, creating it.')
        os.mkdir(directory_path)


# Dropbox functions
def get_dropbox_instance(user: str, app_key: str, app_secret: str) -> Dropbox:
    try:
        access_token, refresh_token = get_tokens(user)
        dbx = Dropbox(
            access_token,
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret
        )
    except FileNotFoundError:
        # TODO: Catch error when refresh token is not valid anymore,
        # or if the tokens could not be parsed or whatever.
        dbx = request_dropbox_instance(user, app_key, app_secret)

    return dbx


def request_dropbox_instance(user: str, app_key: str, app_secret: str) -> Dropbox:
    auth_flow = DropboxOAuth2FlowNoRedirect(
        app_key,
        consumer_secret=app_secret,
        token_access_type="offline",
        scope=["files.content.read"],
    )

    authorize_url = auth_flow.start()
    print("1. Go to: " + authorize_url)
    print('2. Click "Allow" (you might have to log in first).')
    print("3. Copy the authorization code.")
    auth_code = input("Enter the authorization code here: ").strip()

    oauth_result = auth_flow.finish(auth_code)
    # Oauth token has files.metadata.read scope only

    store_tokens(user, oauth_result)

    access_token = oauth_result.access_token
    refresh_token = oauth_result.refresh_token

    return Dropbox(access_token, oauth2_refresh_token=refresh_token, app_key=app_key, app_secret=app_secret)


def get_tokens(user: str):
    with open(DROPBOX_TOKEN_PATH_FORMAT.format(user=user), "r") as f:
        tokens = json.loads(f.read())
        return tokens["access_token"], tokens["refresh_token"]


def store_tokens(user: str, oauth_result):
    with open(DROPBOX_TOKEN_PATH_FORMAT.format(user=user), "w") as f:
        f.write(
            json.dumps(
                {
                    "access_token": oauth_result.access_token,
                    "refresh_token": oauth_result.refresh_token,
                }
            )
        )

def get_md5sum(filepath):
    with open(filepath, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()


# Map functions
def map_series(series):
    return pd.cut(
        series,
        np.linspace(0, 10, 5),
        right=True,
        include_lowest=True,
        labels=range(1, 5)
    )


def map_series_reverse(series):
    return pd.cut(
        series,
        np.linspace(0, 10, 5),
        right=True,
        include_lowest=True,
        labels=range(4, 0, -1)
    )


def map_series_american_to_iso_date(series: pd.Series):
    return series.map(lambda x: f"{x[0:4]}-{x[8:10]}-{x[5:7]}")


if __name__ == "__main__":
    main()
