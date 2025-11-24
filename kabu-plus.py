import sys
import io
from pathlib import Path
import sqlite3
import requests
import pandas as pd
import myenv
import jpx

"""
以下のデータをkabu+から取得してSQLiteに保存する。
https://csvex.com/kabu.plus/csv/tosho-stock-ohlc/daily/ 株価四本値データ
https://csvex.com/kabu.plus/csv/japan-all-stock-data/daily/  投資指標データ
https://csvex.com/kabu.plus/csv/tosho-index-data/daily/ 東証指数データ
https://csvex.com/kabu.plus/csv/japan-all-stock-financial-results/monthly/ 決算・財務・業績データ
"""

CSVEX_URL = "https://csvex.com/kabu.plus/csv/{path}/{frequency}/{path}_{symbol}.csv"


# -- データを更新する関数 --#
def update_data(stocks: pd.DataFrame, path: str, table_name: str, frequency: str) -> None:

    print(f"{table_name} を更新中...")

    # SQLiteのコネクションを取得
    db_path = Path(__file__).with_name("db.sqlite3")
    conn = sqlite3.connect(db_path)

    # pathの名前のテーブルの日付カラムの最大を取得し１日進める
    mydate = None
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone():
        # テーブルが存在しない場合は1年前の同じ月の1日を取得
        mydate = pd.to_datetime("today") - pd.DateOffset(years=1)
        mydate = mydate.replace(day=1)
    else:
        # テーブルが存在する場合は日付カラムの最大値を取得し１日進める
        cur = conn.cursor()
        cur.execute(f"SELECT MAX(date) FROM '{path}'")
        max_date = cur.fetchone()[0]
        # 整数表現(yyyyMMdd)の日付をdatetimeに変換
        max_date = pd.to_datetime(str(max_date), format="%Y%m%d")
        if frequency == "daily":
            mydate = pd.to_datetime(max_date) + pd.Timedelta(days=1)
        elif frequency == "monthly":
            # 次の月の1日に設定
            mydate = (max_date + pd.DateOffset(months=1)).replace(day=1)
        else:
            raise NotImplementedError(f"{frequency} の頻度は未実装です")

    # 日付を１日ずつ進めてデータを取得してテーブルに追加する
    while True:
        # 取得する日付のシンボルを作成
        symbol = mydate.strftime("%Y%m%d")

        # データをダウンロード
        df = kabu_plus_download(CSVEX_URL, path, frequency, symbol)

        # データがない場合は終了
        if df is not None:
            print(f"  {symbol} のデータを追加中...")

            # データを整形して、テーブルに追加
            df = restructure_data(stocks, path, df)
            # dateカラムがない場合は追加
            if "日付" not in df.columns:
                df["日付"] = int(symbol)
            df.to_sql(table_name, conn, if_exists="append", index=False)

        # 日付を１日進める
        mydate += pd.Timedelta(days=1)

        # 今日より未来の日付になったら終了
        if mydate > pd.to_datetime("today"):
            conn.close()
            break


def restructure_data(stocks: pd.DataFrame, path: str, df: pd.DataFrame) -> pd.DataFrame:

    if path == "tosho-stock-ohlc":
        # 株価データ整形

        # カラム名を変更
        df = df.rename(columns={"SC": "コード"})

        # codeがstocksに存在しないものは削除
        df = df[df["コード"].isin(stocks["コード"].values)]

        # カラムを選択
        df = df[["コード", "日付", "始値", "高値", "安値", "終値", "出来高"]]

        # 数値型に変換
        for col in ["始値", "高値", "安値", "終値", "出来高"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    elif path == "japan-all-stock-data":
        # 指標データ整形

        # カラム名を変更
        df = df.rename(columns={"SC": "コード"})

        # codeがstocksに存在しないものは削除
        df = df[df["コード"].isin(stocks["コード"].values)]

        # カラムを削除
        df = df.drop(columns=["市場", "業種", "高値日付", "年初来高値", "安値日付", "年初来安値", "最低購入額", "単元株"])

        # 数値型に変換
        for col in [
            "時価総額（百万円）",
            "発行済株式数",
            "配当利回り（予想）",
            "1株配当（予想）",
            "PER（予想）",
            "PBR（実績）",
            "EPS（予想）",
            "BPS（実績）",
        ]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    elif path == "tosho-index-data":
        # 指数データ整形

        # カラム名を変更
        df = df.rename(columns={"SC": "コード"})

        # カラムを選択
        df = df[["コード", "指数名", "日付", "終値"]]

        # 数値型に変換
        for col in ["終値"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    elif path == "japan-all-stock-financial-results":
        # 決算データ整形

        # カラム名を変更
        df = df.rename(columns={"SC": "コード"})

        # codeがstocksに存在しないものは削除
        df = df[df["コード"].isin(stocks["コード"].values)]

        # 数値型に変換
        numeric_columns = [
            "決算期",
            "決算発表日（本決算）",
            "売上高（百万円）",
            "営業利益（百万円）",
            "経常利益（百万円）",
            "当期利益（百万円）",
            "総資産（百万円）",
            "自己資本（百万円）",
            "資本金（百万円）",
            "有利子負債（百万円）",
            "自己資本比率",
            "ROE",
            "ROA",
            "発行済株式数",
        ]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    else:
        raise NotImplementedError(f"{path} のデータ整形は未実装です")

    return df


# -- kabu+からデータをダウンロードする関数 --#
def kabu_plus_download(url: str, path: str, frequency: str, symbol: str) -> pd.DataFrame:

    # CSVEXからデータを取得
    url = url.format(path=path, frequency=frequency, symbol=symbol)
    r = requests.get(url, auth=(myenv.KABU_PLUS_USER, myenv.KABU_PLUS_PASS))

    # 404の場合はデータなしとしてNoneを返す
    if r.status_code == 404:
        return None
    # その他のエラーは例外を発生させる
    r.raise_for_status()

    return pd.read_csv(io.BytesIO(r.content), encoding="shift_jis")


if __name__ == "__main__":

    if len(sys.argv) > 1 and sys.argv[1] == "update":
        print("JPX銘柄一覧を読み込み中...")
        stocks = jpx.load()

        print("各種データを更新中...")
        update_data(stocks, "tosho-stock-ohlc", "株価データ", "daily")
        update_data(stocks, "japan-all-stock-data", "指標データ", "daily")
        update_data(stocks, "tosho-index-data", "指数データ", "daily")
        update_data(stocks, "japan-all-stock-financial-results", "決算データ_毎月", "monthly")
        print("各種データの更新が完了しました。")
    else:
        print("usage: python kabu-plus.py update")
