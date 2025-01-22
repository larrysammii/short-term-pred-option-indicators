# This step could be skipped if you want to use the data from Huggingface Directly.
# Just my personal testing to ingress to my personal database(it's Questdb).

import os
from dotenv import load_dotenv
from questdb.ingress import Sender, IngressError
import pandas as pd


def questdb_ingress(config: str):
    df = pd.read_csv("hf://datasets/gauss314/options-IV-SP500/data_IV_USA.csv")

    df.drop(columns=["Unnamed: 0"], inplace=True)
    df["date"] = pd.to_datetime(df["date"])

    df = df.sort_values(by="date", ascending=False)

    print(df)

    try:
        with Sender.from_conf(config) as sender:
            sender.dataframe(
                df,
                table_name="historical_SP500_option",
                at="date",
            )
    except IngressError as e:
        print(e)


if __name__ == "__main__":
    load_dotenv()
    questdb_ingress(os.getenv("QUESTDB"))
    print("Done")
