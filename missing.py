import pandas as pd
import os 
import argparse

HOMEDIR = os.path.expanduser("~")

def main(folder):
    d = f"{HOMEDIR}/OneDrive - pku.edu.cn/China in Transition/openrice/data/"
    raw_dir = f"{d}raw/{folder}/csvs/"
    shop_dir = f"{d}shop_meta/{folder}/"
    df_raw = pd.read_csv(fr"{raw_dir}{folder}.csv", encoding="utf-8")
    df_shop = pd.read_csv(fr"{shop_dir}{folder}.csv", encoding="utf-8")
    df_raw = df_raw[df_raw["OpenRice"].fillna("").str.contains("openrice")]
    missing_df=df_raw[~df_raw["id"].isin(df_shop['id'])]
    missing_df.to_csv(fr"{d}missing/{folder}.csv")



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--folder",
        help="should be blue/yellow/green_shop/eat/chains.",
        type=str,
        nargs="?",
        default=None
    )
    args = parser.parse_args()
    main(
        folder=args.folder
    )
