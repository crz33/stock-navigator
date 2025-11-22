import sys
import pandas as pd
import sqlite3

# JPXの東証上場銘柄一覧
JPX_DATA_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"


def download_jpx_data() -> pd.DataFrame:
    # JPXデータをダウンロードする処理をここに実装
    df = pd.read_excel(JPX_DATA_URL)

    # カラム名の変更と削除
    df = df.drop(columns=["日付"])
    df = df.rename(columns={"コード": "code"})
    df = df.rename(columns={"銘柄名": "name"})
    df = df.rename(columns={"市場・商品区分": "market"})
    df = df.rename(columns={"33業種コード": "code33"})
    df = df.drop(columns=["33業種区分"])
    df = df.rename(columns={"17業種コード": "code17"})
    df = df.drop(columns=["17業種区分"])
    df = df.rename(columns={"規模コード": "scale"})
    df = df.drop(columns=["規模区分"])

    # marketをプライム、スタンダード、グロースのみにする
    df = df[
        df["market"].isin(
            ["プライム（内国株式）", "スタンダード（内国株式）", "プライム（内国株式）"]
        )
    ]

    # marketの値を変更
    df["market"] = df["market"].replace(
        {
            "プライム（内国株式）": "P",
            "スタンダード（内国株式）": "S",
            "グロース（内国株式）": "G",
        }
    )

    # DBに書き込む
    conn = sqlite3.connect("db.sqlite3")
    try:
        df.to_sql("stocks", conn, if_exists="replace", index=False)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(download_jpx_data())
