import sys
import pandas as pd
import sqlite3

# JPXの東証上場銘柄一覧
JPX_DATA_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"


def download_jpx_data() -> pd.DataFrame:
    # JPXデータをダウンロードする処理をここに実装
    df = pd.read_excel(JPX_DATA_URL)

    # marketをプライム、スタンダード、グロースのみにする
    df = df[
        df["市場・商品区分"].isin(
            [
                "プライム（内国株式）",
                "スタンダード（内国株式）",
                "グロース（内国株式）",
            ]
        )
    ]

    # marketの値を変更
    df["市場・商品区分"] = df["市場・商品区分"].replace(
        {
            "プライム（内国株式）": "プライム",
            "スタンダード（内国株式）": "スタンダード",
            "グロース（内国株式）": "グロース",
        }
    )

    # DBに書き込む
    conn = sqlite3.connect("db.sqlite3")
    try:
        df.to_sql("銘柄一覧", conn, if_exists="replace", index=False)
    finally:
        conn.close()


def load() -> pd.DataFrame:
    conn = sqlite3.connect("db.sqlite3")
    try:
        df = pd.read_sql("SELECT * FROM 銘柄一覧", conn)
    finally:
        conn.close()
    return df


if __name__ == "__main__":
    sys.exit(download_jpx_data())
