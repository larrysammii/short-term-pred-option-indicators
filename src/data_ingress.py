import pandas as pd


def data_ingress(dataset_link: str) -> pd.DataFrame:
    df = pd.read_csv(dataset_link)
    df.drop(columns=["Unnamed: 0"], inplace=True)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(by="date", ascending=False)

    return df


if __name__ == "__main__":
    link = "hf://datasets/gauss314/options-IV-SP500/data_IV_USA.csv"
    df = data_ingress(link)
    print(df)
